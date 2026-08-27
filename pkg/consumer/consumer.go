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

	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Handler is the function signature for processing a message.
type Handler func(ctx context.Context, m *message.Transferable) error

// Consumer is the public interface implemented by RedisSMQ consumers.
type Consumer interface {
	// ID returns the unique identifier of the consumer.
	ID() string

	// IsRunning reports whether the consumer is currently running.
	IsRunning() bool

	// Consume registers a handler for a queue.
	Consume(queue *queue.Params, handler Handler) Consumer

	// ConsumeWithGroup registers a handler for a Pub/Sub queue and consumer
	// group.
	ConsumeWithGroup(queue *queue.Params, groupID string, handler Handler) Consumer

	// Cancel removes a handler from a queue.
	Cancel(queue *queue.Params) Consumer

	// CancelWithGroup removes a handler from a consumer group.
	CancelWithGroup(queue *queue.Params, groupID string) Consumer

	// Run starts the consumer and all registered handlers.
	Run(ctx context.Context) error

	// Shutdown gracefully stops the consumer.
	Shutdown()

	// Queues returns the queue parameters of all registered handlers.
	Queues() []*queue.Params
}
