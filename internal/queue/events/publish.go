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
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// PublishCreated publishes a queue.queueCreated event to the appropriate
// bus(es) according to the routing policy.
func PublishCreated(ctx context.Context, queue queue.Params, props queue.Props) {
	eventmultiplexer.Publish(ctx, EventCreated, queue, props)
}

// PublishDeleted publishes a queue.queueDeleted event.
func PublishDeleted(ctx context.Context, queue queue.Params) {
	eventmultiplexer.Publish(ctx, EventDeleted, queue)
}

// PublishStateChanged publishes a queue.stateChanged event.
func PublishStateChanged(ctx context.Context, queue queue.Params, transition queue.StateTransition) {
	eventmultiplexer.Publish(ctx, EventStateChanged, queue, transition)
}

// PublishConsumerGroupCreated publishes a queue.consumerGroupCreated event.
func PublishConsumerGroupCreated(ctx context.Context, queue queue.Params, groupID string) {
	eventmultiplexer.Publish(ctx, EventConsumerGroupCreated, queue, groupID)
}

// PublishConsumerGroupDeleted publishes a queue.consumerGroupDeleted event.
func PublishConsumerGroupDeleted(ctx context.Context, queue queue.Params, groupID string) {
	eventmultiplexer.Publish(ctx, EventConsumerGroupDeleted, queue, groupID)
}
