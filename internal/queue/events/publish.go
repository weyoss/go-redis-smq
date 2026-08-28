/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events

import (
	"context"

	"github.com/weyoss/go-redis-smq/internal/eventmultiplexer"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

var log = logger.New("queue", "events")

// publish publishes an event to the appropriate bus(es) and logs any error.
func publish(ctx context.Context, eventName string, args ...interface{}) {
	if err := eventmultiplexer.Publish(ctx, eventName, args...); err != nil {
		log.Error("failed to publish event",
			"event", eventName,
			"error", err,
		)
	}
}

// PublishCreated publishes a queue.queueCreated event.
func PublishCreated(ctx context.Context, queue queue.Params, props queue.Props) {
	publish(ctx, EventCreated, queue, props)
}

// PublishDeleted publishes a queue.queueDeleted event.
func PublishDeleted(ctx context.Context, queue queue.Params) {
	publish(ctx, EventDeleted, queue)
}

// PublishStateChanged publishes a queue.stateChanged event.
func PublishStateChanged(ctx context.Context, queue queue.Params, transition queue.StateTransition) {
	publish(ctx, EventStateChanged, queue, transition)
}

// PublishConsumerGroupCreated publishes a queue.consumerGroupCreated event.
func PublishConsumerGroupCreated(ctx context.Context, queue queue.Params, groupID string) {
	publish(ctx, EventConsumerGroupCreated, queue, groupID)
}

// PublishConsumerGroupDeleted publishes a queue.consumerGroupDeleted event.
func PublishConsumerGroupDeleted(ctx context.Context, queue queue.Params, groupID string) {
	publish(ctx, EventConsumerGroupDeleted, queue, groupID)
}
