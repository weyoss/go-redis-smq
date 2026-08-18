/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package consumer provides the public API for creating and managing
// RedisSMQ consumers.
//
// A consumer subscribes to one or more queues and processes messages using
// user-defined handlers. It manages heartbeats, background workers, and
// graceful shutdown automatically.
package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	internalConsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	consumerEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer/c"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Handler is the function signature for processing a message.
//
// It is an alias of the internal consumer handler type so that external
// users do not need to import internal packages.
type Handler = internalConsumer.Handler

// Consumer is a message consumer.
//
// It receives messages from one or more queues and invokes user-supplied
// handlers. A consumer must be started with Run before it begins processing.
// After use, it should be stopped with Shutdown.
type Consumer struct {
	mu      sync.RWMutex
	id      string
	running bool
	runner  *internalConsumer.MessageHandlerRunner
	options *c.Options
	ctx     context.Context
	cancel  context.CancelFunc
	hb      *internalConsumer.Heartbeat
	log     *slog.Logger
}

// New creates a new consumer instance with the provided options.
//
// By default, the consumer uses a 60-second heartbeat TTL and disabled
// batch acknowledgments/unacknowledgments. Use options like
// c.WithHeartbeatTTL or c.WithBatchAcks to customise behavior.
func New(opts ...c.Option) *Consumer {
	options := c.DefaultOptions()
	for _, o := range opts {
		o(options)
	}

	id := uuid.New().String()
	c := &Consumer{
		id:      id,
		options: options,
		log:     logger.New("consumer", "manager", id),
	}
	c.runner = internalConsumer.NewMessageHandlerRunner(c.id, options)
	return c
}

// ID returns the unique identifier of the consumer.
func (cons *Consumer) ID() string { return cons.id }

// IsRunning reports whether the consumer is currently running.
func (cons *Consumer) IsRunning() bool {
	cons.mu.RLock()
	defer cons.mu.RUnlock()
	return cons.running
}

// Consume registers a message handler for a queue.
//
// The handler is called for each message dequeued from the queue. If the
// handler returns an error, the message is unacknowledged and may be retried
// or dead-lettered according to its retry policy.
func (cons *Consumer) Consume(queue *q.QueueParams, handler Handler) *Consumer {
	cons.log.Debug("adding handler", "queue", queue.String())
	cons.runner.AddHandler(queue, "", handler)
	if cons.IsRunning() {
		cons.log.Debug("starting handler on running consumer", "queue", queue.String())
		cons.runner.StartHandler(queue, "")
	}
	return cons
}

// ConsumeWithGroup registers a message handler for a Pub/Sub queue and
// consumer group.
//
// The handler is called for messages delivered to the specified consumer
// group. If the queue is not Pub/Sub, an error is returned when the consumer
// starts.
func (cons *Consumer) ConsumeWithGroup(queue *q.QueueParams, groupID string, handler Handler) *Consumer {
	cons.log.Debug("adding handler with group", "queue", queue.String(), "group", groupID)
	cons.runner.AddHandler(queue, groupID, handler)
	if cons.IsRunning() {
		cons.log.Debug("starting handler with group on running consumer", "queue", queue.String(), "group", groupID)
		cons.runner.StartHandler(queue, groupID)
	}
	return cons
}

// Cancel removes a previously registered message handler from a queue.
func (cons *Consumer) Cancel(queue *q.QueueParams) *Consumer {
	cons.log.Debug("cancelling handler", "queue", queue.String())
	cons.runner.RemoveHandler(queue, "")
	return cons
}

// CancelWithGroup removes a previously registered message handler from a
// Pub/Sub consumer group.
func (cons *Consumer) CancelWithGroup(queue *q.QueueParams, groupID string) *Consumer {
	cons.log.Debug("cancelling handler with group", "queue", queue.String(), "group", groupID)
	cons.runner.RemoveHandler(queue, groupID)
	return cons
}

// Run starts the consumer and all registered message handlers.
//
// It is safe to call multiple times. If no handlers are registered, it
// returns c.ErrNoQueues.
func (cons *Consumer) Run(ctx context.Context) error {
	cons.mu.Lock()
	defer cons.mu.Unlock()

	if cons.running {
		cons.log.Debug("already running")
		return nil
	}

	if !cons.runner.HasHandlers() {
		cons.log.Warn("no handlers registered")
		return c.ErrNoQueues
	}

	cons.log.Info("starting consumer")

	consumerEvents.PublishGoingUp(ctx, cons.id)

	cons.ctx, cons.cancel = context.WithCancel(ctx)

	if err := cons.runner.Run(cons.ctx); err != nil {
		cons.log.Error("failed to run handlers", "error", err)
		cons.cancel()
		cons.cancel = nil
		consumerEvents.PublishDown(ctx, cons.id)
		return fmt.Errorf("consumer: run handlers: %w", err)
	}

	cons.register()
	cons.hb = internalConsumer.NewHeartbeat(internalConsumer.HeartbeatConfig{
		ID:  cons.id,
		TTL: cons.options.HeartbeatTTL,
	})
	cons.hb.Start(cons.ctx)

	cons.running = true

	// Auto-shutdown when the context is cancelled.
	go func() {
		<-cons.ctx.Done()
		cons.Shutdown()
	}()

	cons.log.Info("consumer started",
		"heartbeatTTL", cons.options.HeartbeatTTL,
		"queues", len(cons.runner.Queues()),
	)

	consumerEvents.PublishUp(cons.ctx, cons.id)

	return nil
}

// Shutdown gracefully stops the consumer.
//
// It returns in-flight messages to the pending queue, stops heartbeats, and
// removes the consumer from all subscribed queues. Shutdown is idempotent
// and safe to call multiple times.
func (cons *Consumer) Shutdown() {
	cons.mu.Lock()
	defer cons.mu.Unlock()
	cons.shutdownLocked()
}

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

// Queues returns the queue parameters for all registered handlers.
func (cons *Consumer) Queues() []*q.QueueParams {
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
