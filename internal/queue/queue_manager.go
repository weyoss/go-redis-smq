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

	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// QueueManager is the concrete implementation of the public queue manager
// interface. It delegates all operations to the internal Manager.
type QueueManager struct {
	manager *Manager
}

// NewQueueManager creates a new concrete queue manager that satisfies the
// public queue.QueueManager interface.
func NewQueueManager() publicqueue.QueueManager {
	return &QueueManager{manager: NewManager()}
}

func (qm *QueueManager) Create(ctx context.Context, params *publicqueue.QueueParams, queueType publicqueue.QueueType, deliveryModel publicqueue.DeliveryModel) error {
	return qm.manager.store.Save(ctx, params, queueType, deliveryModel)
}

func (qm *QueueManager) CreateWithRateLimit(ctx context.Context, params *publicqueue.QueueParams, queueType publicqueue.QueueType, deliveryModel publicqueue.DeliveryModel, rl *publicqueue.RateLimitParams) error {
	return qm.manager.store.SaveWithRateLimit(ctx, params, queueType, deliveryModel, rl)
}

func (qm *QueueManager) Properties(ctx context.Context, params *publicqueue.QueueParams) (*publicqueue.QueueProps, error) {
	return qm.manager.store.Load(ctx, params)
}

func (qm *QueueManager) Exists(ctx context.Context, params *publicqueue.QueueParams) (bool, error) {
	return qm.manager.store.Exists(ctx, params)
}

func (qm *QueueManager) Delete(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.store.Delete(ctx, params)
}

func (qm *QueueManager) ListAll(ctx context.Context) ([]publicqueue.QueueParams, error) {
	return qm.manager.lookup.All(ctx)
}

func (qm *QueueManager) ListByNamespace(ctx context.Context, namespace string) ([]publicqueue.QueueParams, error) {
	return qm.manager.lookup.ByNamespace(ctx, namespace)
}

func (qm *QueueManager) BrowseMessages(ctx context.Context, queueParams *publicqueue.QueueParams, params *publicqueue.BrowseParams) (*publicqueue.BrowseResult, error) {
	return qm.manager.browse.BrowseMessages(ctx, queueParams, params)
}

func (qm *QueueManager) PurgeQueue(ctx context.Context, queueParams *publicqueue.QueueParams, filter publicqueue.BrowseFilter) (string, error) {
	return qm.manager.purge.Enqueue(ctx, queueParams, filter)
}

func (qm *QueueManager) GetPurgeJob(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
	return qm.manager.purge.Get(ctx, jobID)
}

func (qm *QueueManager) CancelPurgeJob(ctx context.Context, queueParams *publicqueue.QueueParams, jobID string) error {
	return qm.manager.purge.Cancel(ctx, queueParams, jobID)
}

func (qm *QueueManager) SetRateLimit(ctx context.Context, params *publicqueue.QueueParams, rl *publicqueue.RateLimitParams) error {
	return qm.manager.store.SetRateLimit(ctx, params, rl)
}

func (qm *QueueManager) ClearRateLimit(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.store.ClearRateLimit(ctx, params)
}

func (qm *QueueManager) RateLimit(ctx context.Context, params *publicqueue.QueueParams) (*publicqueue.RateLimitParams, error) {
	return qm.manager.store.GetRateLimit(ctx, params)
}

func (qm *QueueManager) MustExist(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.validator.Exists(ctx, params)
}

func (qm *QueueManager) MustBeOperational(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.validator.IsOperational(ctx, params)
}

func (qm *QueueManager) CanEnqueue(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.validator.CanEnqueue(ctx, params)
}

func (qm *QueueManager) CanDequeue(ctx context.Context, params *publicqueue.QueueParams) error {
	return qm.manager.validator.CanDequeue(ctx, params)
}
