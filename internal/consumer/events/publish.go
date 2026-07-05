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

	"github.com/weyoss/go-redis-smq/internal/eventbus"
)

func PublishUp(ctx context.Context, payload LifecyclePayload) error {
	return eventbus.Singleton().Publish(ctx, EventUp, payload)
}

func PublishDown(ctx context.Context, payload LifecyclePayload) error {
	return eventbus.Singleton().Publish(ctx, EventDown, payload)
}

func PublishGoingUp(ctx context.Context, payload LifecyclePayload) error {
	return eventbus.Singleton().Publish(ctx, EventGoingUp, payload)
}

func PublishGoingDown(ctx context.Context, payload LifecyclePayload) error {
	return eventbus.Singleton().Publish(ctx, EventGoingDown, payload)
}

func PublishMessageReceived(ctx context.Context, payload MessageReceivedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageReceived, payload)
}

func PublishMessageAcknowledged(ctx context.Context, payload MessagePayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageAcknowledged, payload)
}

func PublishMessageUnacknowledged(ctx context.Context, payload MessageUnacknowledgedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageUnacknowledged, payload)
}

func PublishMessageDeadLettered(ctx context.Context, payload MessageDeadLetteredPayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageDeadLettered, payload)
}

func PublishMessageRequeued(ctx context.Context, payload MessagePayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageRequeued, payload)
}

func PublishMessageDelayed(ctx context.Context, payload MessagePayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessageDelayed, payload)
}
