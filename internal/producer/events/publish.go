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

func PublishMessagePublished(ctx context.Context, payload MessagePublishedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventMessagePublished, payload)
}
