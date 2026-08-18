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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// PublishUp publishes a consumer.up event to the public user bus.
func PublishUp(ctx context.Context, consumerID string) {
	eventmultiplexer.Publish(ctx, EventUp, consumerID)
}

// PublishDown publishes a consumer.down event to the public user bus.
func PublishDown(ctx context.Context, consumerID string) {
	eventmultiplexer.Publish(ctx, EventDown, consumerID)
}

// PublishGoingUp publishes a consumer.goingUp event to the public user bus.
func PublishGoingUp(ctx context.Context, consumerID string) {
	eventmultiplexer.Publish(ctx, EventGoingUp, consumerID)
}

// PublishGoingDown publishes a consumer.goingDown event to the public user bus.
func PublishGoingDown(ctx context.Context, consumerID string) {
	eventmultiplexer.Publish(ctx, EventGoingDown, consumerID)
}

// PublishMessageReceived publishes a consumer.messageReceived event to the
// public user bus.
func PublishMessageReceived(ctx context.Context, messageID string, queue q.QueueParams, consumerID string) {
	eventmultiplexer.Publish(ctx, EventMessageReceived, messageID, queue, consumerID)
}

// PublishMessageAcknowledged publishes a consumer.messageAcknowledged event
// to the public user bus.
func PublishMessageAcknowledged(ctx context.Context, messageID string, queue q.QueueParams, consumerID string) {
	eventmultiplexer.Publish(ctx, EventMessageAcknowledged, messageID, queue, consumerID)
}

// PublishMessageUnacknowledged publishes a consumer.messageUnacknowledged
// event to the public user bus.
func PublishMessageUnacknowledged(ctx context.Context, messageID string, queue q.QueueParams, consumerID string, cause int) {
	eventmultiplexer.Publish(ctx, EventMessageUnacknowledged, messageID, queue, consumerID, cause)
}

// PublishMessageDeadLettered publishes a consumer.messageDeadLettered event
// to the public user bus.
func PublishMessageDeadLettered(ctx context.Context, messageID string, queue q.QueueParams, consumerID string, cause int) {
	eventmultiplexer.Publish(ctx, EventMessageDeadLettered, messageID, queue, consumerID, cause)
}

// PublishMessageRequeued publishes a consumer.messageRequeued event to the
// public user bus.
func PublishMessageRequeued(ctx context.Context, messageID string, queue q.QueueParams, consumerID string) {
	eventmultiplexer.Publish(ctx, EventMessageRequeued, messageID, queue, consumerID)
}

// PublishMessageDelayed publishes a consumer.messageDelayed event to the
// public user bus.
func PublishMessageDelayed(ctx context.Context, messageID string, queue q.QueueParams, consumerID string) {
	eventmultiplexer.Publish(ctx, EventMessageDelayed, messageID, queue, consumerID)
}
