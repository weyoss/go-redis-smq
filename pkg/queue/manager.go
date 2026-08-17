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

	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Manager provides a high-level interface for queue‑related operations.
// It wraps the internal Redis‑backed queue manager and exposes methods for
// browsing, purging, and inspecting queues.
type Manager struct {
	store     *internalQueue.Store
	lookup    *internalQueue.Lookup
	validator *internalQueue.Validator
	browse    *internalQueue.Browse
	purge     *internalQueue.PurgeManager
}

// NewManager creates a new queue manager with default codecs.
// It uses the default Redis-backed internal manager.
func NewManager() *Manager {
	rdbManager := internalQueue.NewManager()
	return &Manager{
		store:     rdbManager.Store(),
		lookup:    rdbManager.Lookup(),
		validator: rdbManager.Validator(),
		browse:    rdbManager.Browse(),
		purge:     rdbManager.Purge(),
	}
}

// defaultManager is the shared instance used by package‑level
// convenience functions.
var defaultManager = NewManager()

// GetQueueProps returns the properties of a queue using the default manager.
func GetQueueProps(ctx context.Context, params *q.QueueParams) (*q.QueueProps, error) {
	return defaultManager.Properties(ctx, params)
}

// BrowseMessages returns a page of message IDs from a queue.
func (m *Manager) BrowseMessages(ctx context.Context, queueParams *q.QueueParams, params *q.BrowseParams) (*q.BrowseResult, error) {
	return m.browse.BrowseMessages(ctx, queueParams, params)
}

// BrowseMessages returns a page of message IDs using the default manager.
func BrowseMessages(ctx context.Context, queueParams *q.QueueParams, params *q.BrowseParams) (*q.BrowseResult, error) {
	return defaultManager.BrowseMessages(ctx, queueParams, params)
}

// PurgeQueue enqueues a background purge job for a queue.
func (m *Manager) PurgeQueue(ctx context.Context, queueParams *q.QueueParams, filter q.BrowseFilter) (string, error) {
	return m.purge.Enqueue(ctx, queueParams, filter)
}

// PurgeQueue enqueues a purge job using the default manager.
func PurgeQueue(ctx context.Context, queueParams *q.QueueParams, filter q.BrowseFilter) (string, error) {
	return defaultManager.PurgeQueue(ctx, queueParams, filter)
}

// GetPurgeJob returns a purge job by its ID.
func (m *Manager) GetPurgeJob(ctx context.Context, jobID string) (*q.PurgeJob, error) {
	return m.purge.Get(ctx, jobID)
}

// GetPurgeJob returns a purge job using the default manager.
func GetPurgeJob(ctx context.Context, jobID string) (*q.PurgeJob, error) {
	return defaultManager.GetPurgeJob(ctx, jobID)
}

// CancelPurgeJob cancels a pending or running purge job.
func (m *Manager) CancelPurgeJob(ctx context.Context, queueParams *q.QueueParams, jobID string) error {
	return m.purge.Cancel(ctx, queueParams, jobID)
}

// CancelPurgeJob cancels a purge job using the default manager.
func CancelPurgeJob(ctx context.Context, queueParams *q.QueueParams, jobID string) error {
	return defaultManager.CancelPurgeJob(ctx, queueParams, jobID)
}
