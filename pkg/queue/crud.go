/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"context"

	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Create creates a new queue with the specified type and delivery model.
//
// Example:
//
//	params := q.MustQueueParamsWithNS("orders", "production")
//	err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
func (m *Manager) Create(ctx context.Context, params *q.QueueParams, queueType q.QueueType, deliveryModel q.DeliveryModel) error {
	return m.store.Save(ctx, params, queueType, deliveryModel)
}

// CreateWithRateLimit creates a new queue with rate limiting enabled.
//
// Example:
//
//	rl := q.MustRateLimitParams(100, time.Minute)
//	err := queue.NewManager().CreateWithRateLimit(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint, rl)
func (m *Manager) CreateWithRateLimit(
	ctx context.Context,
	params *q.QueueParams,
	queueType q.QueueType,
	deliveryModel q.DeliveryModel,
	rl *q.RateLimitParams,
) error {
	return m.store.SaveWithRateLimit(ctx, params, queueType, deliveryModel, rl)
}

// Properties retrieves stored queue configuration.
//
// Example:
//
//	props, err := queue.Properties(ctx, params)
//	fmt.Println(props.Type, props.OperationalState)
func (m *Manager) Properties(ctx context.Context, params *q.QueueParams) (*q.QueueProps, error) {
	return m.store.Load(ctx, params)
}

// Exists checks whether a queue has been created.
func (m *Manager) Exists(ctx context.Context, params *q.QueueParams) (bool, error) {
	return m.store.Exists(ctx, params)
}

// Delete removes a queue and all associated data.
//
// This includes messages, consumer groups, exchange bindings, and state
// history. The queue must be empty and have no active consumers.
func (m *Manager) Delete(ctx context.Context, params *q.QueueParams) error {
	return m.store.Delete(ctx, params)
}

// Create creates a queue using the default manager.
func Create(ctx context.Context, params *q.QueueParams, queueType q.QueueType, deliveryModel q.DeliveryModel) error {
	return defaultManager.Create(ctx, params, queueType, deliveryModel)
}

// Properties retrieves queue properties using the default manager.
func Properties(ctx context.Context, params *q.QueueParams) (*q.QueueProps, error) {
	return defaultManager.Properties(ctx, params)
}

// Exists checks queue existence using the default manager.
func Exists(ctx context.Context, params *q.QueueParams) (bool, error) {
	return defaultManager.Exists(ctx, params)
}

// Delete removes a queue using the default manager.
func Delete(ctx context.Context, params *q.QueueParams) error {
	return defaultManager.Delete(ctx, params)
}
