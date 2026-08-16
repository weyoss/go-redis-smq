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

var defaultManager = NewManager()

func GetQueueProps(ctx context.Context, params *q.QueueParams) (*q.QueueProps, error) {
	return defaultManager.Properties(ctx, params)
}

func (m *Manager) BrowseMessages(ctx context.Context, queueParams *q.QueueParams, params *q.BrowseParams) (*q.BrowseResult, error) {
	return m.browse.BrowseMessages(ctx, queueParams, params)
}

func BrowseMessages(ctx context.Context, queueParams *q.QueueParams, params *q.BrowseParams) (*q.BrowseResult, error) {
	return defaultManager.BrowseMessages(ctx, queueParams, params)
}

func (m *Manager) PurgeQueue(ctx context.Context, queueParams *q.QueueParams, filter q.BrowseFilter) (string, error) {
	return m.purge.Enqueue(ctx, queueParams, filter)
}

func PurgeQueue(ctx context.Context, queueParams *q.QueueParams, filter q.BrowseFilter) (string, error) {
	return defaultManager.PurgeQueue(ctx, queueParams, filter)
}

func (m *Manager) GetPurgeJob(ctx context.Context, jobID string) (*q.PurgeJob, error) {
	return m.purge.Get(ctx, jobID)
}

func GetPurgeJob(ctx context.Context, jobID string) (*q.PurgeJob, error) {
	return defaultManager.GetPurgeJob(ctx, jobID)
}

func (m *Manager) CancelPurgeJob(ctx context.Context, queueParams *q.QueueParams, jobID string) error {
	return m.purge.Cancel(ctx, queueParams, jobID)
}

func CancelPurgeJob(ctx context.Context, queueParams *q.QueueParams, jobID string) error {
	return defaultManager.CancelPurgeJob(ctx, queueParams, jobID)
}
