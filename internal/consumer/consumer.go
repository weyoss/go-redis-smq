/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	consumerEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicconsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type Consumer struct {
	mu      sync.RWMutex
	id      string
	running bool
	runner  *MessageHandlerRunner
	options *publicconsumer.Options
	ctx     context.Context
	cancel  context.CancelFunc
	hb      *Heartbeat
	log     *slog.Logger
}

// New creates a new consumer that satisfies the public consumer interface.
func New(opts ...publicconsumer.Option) publicconsumer.Consumer {
	options := publicconsumer.DefaultOptions()
	for _, o := range opts {
		o(options)
	}

	id := uuid.New().String()
	return &Consumer{
		id:      id,
		options: options,
		log:     logger.New("consumer", "manager", id),
		runner:  NewMessageHandlerRunner(id, options),
	}
}

func (cons *Consumer) ID() string { return cons.id }

func (cons *Consumer) IsRunning() bool {
	cons.mu.RLock()
	defer cons.mu.RUnlock()
	return cons.running
}

func (cons *Consumer) Consume(queue *queue.Params, handler publicconsumer.Handler) publicconsumer.Consumer {
	cons.log.Debug("adding handler", "queue", queue.String())
	cons.runner.AddHandler(queue, "", Handler(handler))
	if cons.IsRunning() {
		cons.log.Debug("starting handler on running consumer", "queue", queue.String())
		cons.runner.StartHandler(queue, "")
	}
	return cons
}

func (cons *Consumer) ConsumeWithGroup(queue *queue.Params, groupID string, handler publicconsumer.Handler) publicconsumer.Consumer {
	cons.log.Debug("adding handler with group", "queue", queue.String(), "group", groupID)
	cons.runner.AddHandler(queue, groupID, Handler(handler))
	if cons.IsRunning() {
		cons.log.Debug("starting handler with group on running consumer", "queue", queue.String(), "group", groupID)
		cons.runner.StartHandler(queue, groupID)
	}
	return cons
}

func (cons *Consumer) Cancel(queue *queue.Params) publicconsumer.Consumer {
	cons.log.Debug("cancelling handler", "queue", queue.String())
	cons.runner.RemoveHandler(queue, "")
	return cons
}

func (cons *Consumer) CancelWithGroup(queue *queue.Params, groupID string) publicconsumer.Consumer {
	cons.log.Debug("cancelling handler with group", "queue", queue.String(), "group", groupID)
	cons.runner.RemoveHandler(queue, groupID)
	return cons
}

// Run starts the consumer and all registered handlers.
//
// Run is transactional on the runner startup failure path: if
// runner.Run returns an error, cons.shutdownLocked's !running branch
// unwinds the partial state (ctx, heartbeat if any, runner instances)
// before Run returns. The failure path also publishes the matching
// consumer.down event to close the consumer.goingUp pair opened at
// the top of Run.
func (cons *Consumer) Run(ctx context.Context) error {
	cons.mu.Lock()
	defer cons.mu.Unlock()

	if cons.running {
		cons.log.Debug("already running")
		return nil
	}

	if !cons.runner.HasHandlers() {
		cons.log.Warn("no handlers registered")
		return publicconsumer.ErrNoQueues
	}

	cons.log.Info("starting consumer")

	consumerEvents.PublishGoingUp(ctx, cons.id)

	cons.ctx, cons.cancel = context.WithCancel(ctx)
	// Capture cons.ctx into a local variable: the shutdown goroutine below
	// must observe the ctx created by THIS Run, not whatever cons.ctx points
	// to at the moment the goroutine executes. A subsequent Run (after
	// Shutdown) overwrites cons.ctx under cons.mu; without the local capture
	// the old goroutine could read the new ctx.
	shutdownCtx := cons.ctx

	if err := cons.runner.Run(shutdownCtx); err != nil {
		cons.log.Error("failed to run handlers", "error", err)
		// Delegate cleanup to shutdownLocked's !running branch: cancels
		// cons.ctx, stops heartbeat (no-op — never started), calls
		// unregister (DEL on never-written keys, no-op), calls
		// runner.Shutdown (idempotent — runner.Run already rolled back
		// its own partial state on its failure paths).
		//
		// The runner's own failure paths (in runner.Run) call r.Shutdown,
		// which nils out r.instances. Here, the extra runner.Shutdown
		// is a defence against future changes to runner.Run that might
		// miss a path.
		cons.shutdownLocked()
		consumerEvents.PublishDown(ctx, cons.id)
		return fmt.Errorf("consumer: run handlers: %w", err)
	}

	cons.register()
	cons.hb = NewHeartbeat(HeartbeatConfig{
		ID:  cons.id,
		TTL: cons.options.HeartbeatTTL,
	})
	cons.hb.Start(shutdownCtx)

	cons.running = true

	go func() {
		<-shutdownCtx.Done()
		cons.Shutdown()
	}()

	cons.log.Info("consumer started",
		"heartbeatTTL", cons.options.HeartbeatTTL,
		"queues", len(cons.runner.Queues()),
	)

	consumerEvents.PublishUp(shutdownCtx, cons.id)

	return nil
}

func (cons *Consumer) Shutdown() {
	cons.mu.Lock()
	defer cons.mu.Unlock()
	cons.shutdownLocked()
}

// shutdownLocked performs the shutdown sequence. Caller must hold cons.mu.
//
// Two branches:
//
//   - !running: the consumer was never fully started (either Run hasn't
//     completed, or Run failed before setting running = true). Performs
//     the partial cleanup: cancel ctx, stop heartbeat, unregister, shut
//     down the runner. Publishes consumer.down so the matching
//     consumer.goingUp event from Run has a paired close.
//
//   - running: full cleanup with the goingDown/down event pair.
//
// Idempotent in both branches: all cleanup steps are nil-safe.
func (cons *Consumer) shutdownLocked() {
	if !cons.running {
		cons.log.Debug("shutdown called but not running — cleaning up partial state")
		if cons.cancel != nil {
			cons.cancel()
			cons.cancel = nil
		}
		if cons.hb != nil {
			cons.hb.Stop()
			cons.hb = nil
		}
		cons.unregister()
		cons.runner.Shutdown()
		// Close the lifecycle event pair. PublishGoingUp was fired at the
		// top of Run; without a matching PublishDown here, subscribers see
		// a dangling "goingUp" event when Run fails before running = true
		// is set. The full-running path publishes its own down event below.
		consumerEvents.PublishDown(context.Background(), cons.id)
		return
	}

	cons.log.Info("shutting down consumer")

	consumerEvents.PublishGoingDown(context.Background(), cons.id)

	cons.running = false

	if cons.hb != nil {
		cons.hb.Stop()
		cons.hb = nil
	}

	cons.runner.Shutdown()
	cons.unregister()

	if cons.cancel != nil {
		cons.cancel()
		cons.cancel = nil
	}

	cons.log.Info("consumer shut down complete")

	consumerEvents.PublishDown(context.Background(), cons.id)
}

func (cons *Consumer) Queues() []*queue.Params {
	return cons.runner.Queues()
}

func (cons *Consumer) register() {
	ctx := context.Background()
	consumerKey := redisKeys.System{}.ConsumerHeartbeat(cons.id)
	redisClient.Client().Set(ctx, consumerKey, "{}", cons.options.HeartbeatTTL)
	cons.log.Debug("registered heartbeat key", "key", consumerKey)
}

func (cons *Consumer) unregister() {
	ctx := context.Background()
	heartbeatKey := redisKeys.System{}.ConsumerHeartbeat(cons.id)
	queuesKey := redisKeys.System{}.ConsumerQueues(cons.id)
	redisClient.Client().Del(ctx, heartbeatKey, queuesKey)
	cons.log.Debug("unregistered consumer keys")
}
