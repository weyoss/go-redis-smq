/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"context"

	"github.com/weyoss/go-redis-smq/internal/eventmultiplexer"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// PublishUp publishes a producer.up event to the public user bus.
func PublishUp(ctx context.Context, producerID string) {
	eventmultiplexer.Publish(ctx, EventUp, producerID)
}

// PublishDown publishes a producer.down event to the public user bus.
func PublishDown(ctx context.Context, producerID string) {
	eventmultiplexer.Publish(ctx, EventDown, producerID)
}

// PublishGoingUp publishes a producer.goingUp event to the public user bus.
func PublishGoingUp(ctx context.Context, producerID string) {
	eventmultiplexer.Publish(ctx, EventGoingUp, producerID)
}

// PublishGoingDown publishes a producer.goingDown event to the public user bus.
func PublishGoingDown(ctx context.Context, producerID string) {
	eventmultiplexer.Publish(ctx, EventGoingDown, producerID)
}

// PublishMessagePublished publishes a producer.messagePublished event to the
// public user bus.
//
// The arguments match the TypeScript event signature:
//
//	(messageId: string, queue: IQueueParsedParams, producerId: string) => void
func PublishMessagePublished(ctx context.Context, messageID string, queue q.QueueParams, producerID string) {
	eventmultiplexer.Publish(ctx, EventMessagePublished, messageID, queue, producerID)
}
