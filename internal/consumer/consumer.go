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

// Consume registers a handler for a queue.
//
// If the consumer is already running, the handler is started immediately.
// Otherwise it will be picked up on the next Run. Concurrent Consume + Run
// has a narrow window where a handler added after Run's HasHandlers check
// is not started until the next Run or reconcile cycle — this is a
// documented edge case, not a bug; the recommended usage is to register
// all handlers before calling Run, or use Consume on a running consumer.
func (cons *Consumer) Consume(queue *queue.Params, handler publicconsumer.Handler) publicconsumer.Consumer {
	cons.log.Debug("adding handler", "queue", queue.String())
	cons.runner.AddHandler(queue, "", Handler(handler))
	if cons.IsRunning() {
		cons.log.Debug("starting handler on running consumer", "queue", queue.String())
		cons.runner.StartHandler(queue, "")
	}
	return cons
}

// ConsumeWithGroup registers a handler for a Pub/Sub queue and consumer group.
// See Consume for the concurrent Run + Consume caveat.
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
// before Run returns. The failure path publishes consumer.down itself
// (not from shutdownLocked) so the event pair opened by PublishGoingUp
// is closed exactly once — see shutdownLocked's doc for why.
//
// All four lifecycle events (goingUp, up, goingDown, down) are published
// on context.Background() rather than the caller's ctx. Rationale: the
// caller's ctx may be cancelled mid-lifecycle (a common cause of Run
// failure, or of Shutdown being triggered); publishing on a cancelled
// context causes the event to be silently dropped, defeating the
// event's purpose. Subscribers should always observe a well-formed
// lifecycle sequence.
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

	consumerEvents.PublishGoingUp(context.Background(), cons.id)

	cons.ctx, cons.cancel = context.WithCancel(ctx)
	// Capture cons.ctx into a local variable: the shutdown goroutine below
	// must observe the ctx created by THIS Run, not whatever cons.ctx points
	// to at the moment the goroutine executes. A subsequent Run (after
	// Shutdown) overwrites cons.ctx under cons.mu; without the local capture
	// the old goroutine could read the new ctx.
	shutdownCtx := cons.ctx

	if err := cons.runner.Run(shutdownCtx); err != nil {
		cons.log.Error("failed to run handlers", "error", err)
		// Partial cleanup. shutdownLocked's !running branch performs the
		// actual work (cancel ctx, stop hb, runner.Shutdown); it does NOT
		// publish a down event.
		cons.shutdownLocked()
		// Close the lifecycle pair opened by PublishGoingUp above. The
		// running branch of shutdownLocked owns this event when a consumer
		// actually runs; here, Run failed before running = true was set,
		// so Run owns it. Publishing from shutdownLocked's !running branch
		// would double-publish when the watcher fires after a clean
		// shutdown (see TestConsumerEvents_Lifecycle).
		consumerEvents.PublishDown(context.Background(), cons.id)
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

	consumerEvents.PublishUp(context.Background(), cons.id)

	return nil
}

func (cons *Consumer) Shutdown() {
	cons.mu.Lock()
	defer cons.mu.Unlock()
	cons.shutdownLocked()
}

// shutdownLocked performs the shutdown sequence. Caller must hold cons.mu.
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
		// Do NOT call cons.unregister() here — that was C10. unregister()
		// touches Redis, and this branch is reachable from the watcher
		// goroutine after internalredis.Close() has nilled the client.
		// See the doc comment above for the full invariant.
		//
		// unregister() is not needed in this branch under any path:
		//   - Clean shutdown: running branch already called unregister(),
		//     which deleted the keys. The watcher's second pass has nothing
		//     to do.
		//   - Run failure: register() is called only after runner.Run
		//     succeeds, so it never ran. No keys were created.
		cons.runner.Shutdown()
		// No event publish here. See the event ownership rule above.
		return
	}

	cons.log.Info("shutting down consumer")

	consumerEvents.PublishGoingDown(context.Background(), cons.id)

	cons.running = false

	// hb.Stop internally DELs the heartbeat key. unregister below DELs the
	// same key (plus ConsumerQueues) again — redundant but idempotent,
	// kept as defence in depth in case hb.Stop's Redis call errored. Not
	// worth restructuring.
	if cons.hb != nil {
		cons.hb.Stop()
		cons.hb = nil
	}

	cons.runner.Shutdown()
	cons.unregister()

	// Cancel the ctx last. This wakes the watcher goroutine spawned in Run
	// (which calls cons.Shutdown() again) — that re-entry is safe because
	// running is already false, so it takes the !running branch and no-ops
	//
	// Ordering reads backwards (cancel normally comes before cleanup), but
	// cleanup here is done by the components we explicitly tore down above
	// (hb.Stop, runner.Shutdown), not by ctx cancellation propagation.
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

// register writes the initial consumer heartbeat key and logs any error.
//
// Failure here is not fatal — the heartbeat beat loop will retry on its
// next tick (interval = HeartbeatTTL/3). But logging the error is
// important for observability: a transient Redis unavailability during
// startup would otherwise go unnoticed.
func (cons *Consumer) register() {
	ctx := context.Background()
	consumerKey := redisKeys.System{}.ConsumerHeartbeat(cons.id)
	if err := redisClient.Client().Set(ctx, consumerKey, "{}", cons.options.HeartbeatTTL).Err(); err != nil {
		cons.log.Error("failed to register heartbeat key", "key", consumerKey, "error", err)
		return
	}
	cons.log.Debug("registered heartbeat key", "key", consumerKey)
}

// unregister removes the consumer's heartbeat and queue-membership keys.
// Errors are logged but not returned — this is called during shutdown
// where the caller cannot meaningfully respond to a failure.
func (cons *Consumer) unregister() {
	ctx := context.Background()
	heartbeatKey := redisKeys.System{}.ConsumerHeartbeat(cons.id)
	queuesKey := redisKeys.System{}.ConsumerQueues(cons.id)
	if err := redisClient.Client().Del(ctx, heartbeatKey, queuesKey).Err(); err != nil {
		cons.log.Error("failed to unregister consumer keys", "error", err)
		return
	}
	cons.log.Debug("unregistered consumer keys")
}
