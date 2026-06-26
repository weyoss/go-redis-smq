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

func PublishCreated(ctx context.Context, payload CreatedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventCreated, payload)
}

func PublishDeleted(ctx context.Context, payload DeletedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventDeleted, payload)
}

func PublishStateChanged(ctx context.Context, payload StateChangedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventStateChanged, payload)
}

func PublishConsumerGroupCreated(ctx context.Context, payload ConsumerGroupCreatedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventConsumerGroupCreated, payload)
}

func PublishConsumerGroupDeleted(ctx context.Context, payload ConsumerGroupDeletedPayload) error {
	return eventbus.Singleton().Publish(ctx, EventConsumerGroupDeleted, payload)
}
