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
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

var log = logger.New("producer", "events")

// publish publishes an event to the appropriate bus(es) and logs any error.
func publish(ctx context.Context, eventName string, args ...interface{}) {
	if err := eventmultiplexer.Publish(ctx, eventName, args...); err != nil {
		log.Error("failed to publish event",
			"event", eventName,
			"error", err,
		)
	}
}

// PublishUp publishes a producer.up event to the public user bus.
func PublishUp(ctx context.Context, producerID string) {
	publish(ctx, EventUp, producerID)
}

// PublishDown publishes a producer.down event to the public user bus.
func PublishDown(ctx context.Context, producerID string) {
	publish(ctx, EventDown, producerID)
}

// PublishGoingUp publishes a producer.goingUp event to the public user bus.
func PublishGoingUp(ctx context.Context, producerID string) {
	publish(ctx, EventGoingUp, producerID)
}

// PublishGoingDown publishes a producer.goingDown event to the public user bus.
func PublishGoingDown(ctx context.Context, producerID string) {
	publish(ctx, EventGoingDown, producerID)
}

// PublishMessagePublished publishes a producer.messagePublished event to the
// public user bus.
//
// The arguments match the TypeScript event signature:
//
//	(messageId: string, queue: IQueueParsedParams, producerId: string) => void
func PublishMessagePublished(ctx context.Context, messageID string, queue queue.Params, producerID string) {
	publish(ctx, EventMessagePublished, messageID, queue, producerID)
}
