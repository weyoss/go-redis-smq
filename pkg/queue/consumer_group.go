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

// ConsumerGroupManager manages consumer groups for PUB/SUB queues.
//
// Consumer groups enable the Pub/Sub delivery model where each message is
// delivered to all groups, but only to a single consumer within each group.
// The manager provides methods to create, list, and delete these groups.
type ConsumerGroupManager struct {
	store *internalQueue.ConsumerGroupStore
}

// NewConsumerGroupManager creates a new consumer group manager.
//
// It returns a manager that delegates all operations to the internal
// Redis-backed store.
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		store: internalQueue.NewConsumerGroupStore(),
	}
}

// Save creates a consumer group for a queue.
//
// It returns 1 if the group was newly created, or 0 if it already existed.
// The queue must use the Pub/Sub delivery model for groups to be supported.
//
// Example:
//
//	result, err := manager.Save(ctx, queueParams, "email-group")
func (cgm *ConsumerGroupManager) Save(ctx context.Context, queueParams *q.QueueParams, groupID string) (int64, error) {
	return cgm.store.Save(ctx, queueParams, groupID)
}

// Delete removes a consumer group from a queue.
//
// The group must be empty (no pending messages) and have no active
// consumers. The queue must use the Pub/Sub delivery model.
//
// Example:
//
//	err := manager.Delete(ctx, queueParams, "email-group")
func (cgm *ConsumerGroupManager) Delete(ctx context.Context, queueParams *q.QueueParams, groupID string) error {
	return cgm.store.Delete(ctx, queueParams, groupID)
}

// List returns all consumer group IDs for a queue.
//
// Example:
//
//	groups, err := manager.List(ctx, queueParams)
func (cgm *ConsumerGroupManager) List(ctx context.Context, queueParams *q.QueueParams) ([]string, error) {
	return cgm.store.List(ctx, queueParams)
}

// defaultConsumerGroupManager is the shared instance used by the
// package-level convenience functions.
var defaultConsumerGroupManager = NewConsumerGroupManager()

// SaveConsumerGroup creates a consumer group using the default manager.
//
// See ConsumerGroupManager.Save for details.
func SaveConsumerGroup(ctx context.Context, queueParams *q.QueueParams, groupID string) (int64, error) {
	return defaultConsumerGroupManager.Save(ctx, queueParams, groupID)
}

// DeleteConsumerGroup deletes a consumer group using the default manager.
//
// See ConsumerGroupManager.Delete for details.
func DeleteConsumerGroup(ctx context.Context, queueParams *q.QueueParams, groupID string) error {
	return defaultConsumerGroupManager.Delete(ctx, queueParams, groupID)
}

// ListConsumerGroups returns consumer groups using the default manager.
//
// See ConsumerGroupManager.List for details.
func ListConsumerGroups(ctx context.Context, queueParams *q.QueueParams) ([]string, error) {
	return defaultConsumerGroupManager.List(ctx, queueParams)
}
