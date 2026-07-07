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
// Consumer groups allow multiple consumers to process messages from the same queue,
// with each message delivered to all groups.
type ConsumerGroupManager struct {
	store *internalQueue.ConsumerGroupStore
}

// NewConsumerGroupManager creates a new consumer group manager.
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		store: internalQueue.NewConsumerGroupStore(),
	}
}

// Save creates a consumer group for a queue.
// Returns 1 if the group was created, 0 if it already exists.
//
// Example:
//
//	result, err := consumerGroups.Save(ctx, queueParams, "email-group")
func (cgm *ConsumerGroupManager) Save(ctx context.Context, queueParams *q.QueueParams, groupID string) (int64, error) {
	return cgm.store.Save(ctx, queueParams, groupID)
}

// Delete removes a consumer group from a queue.
// The group must be empty and have no active consumers.
//
// Example:
//
//	err := consumerGroups.Delete(ctx, queueParams, "email-group")
func (cgm *ConsumerGroupManager) Delete(ctx context.Context, queueParams *q.QueueParams, groupID string) error {
	return cgm.store.Delete(ctx, queueParams, groupID)
}

// List returns all consumer groups for a queue.
//
// Example:
//
//	groups, err := consumerGroups.List(ctx, queueParams)
func (cgm *ConsumerGroupManager) List(ctx context.Context, queueParams *q.QueueParams) ([]string, error) {
	return cgm.store.List(ctx, queueParams)
}

// Default consumer group manager instance.
var defaultConsumerGroupManager = NewConsumerGroupManager()

// SaveConsumerGroup creates a consumer group using the default manager.
func SaveConsumerGroup(ctx context.Context, queueParams *q.QueueParams, groupID string) (int64, error) {
	return defaultConsumerGroupManager.Save(ctx, queueParams, groupID)
}

// DeleteConsumerGroup deletes a consumer group using the default manager.
func DeleteConsumerGroup(ctx context.Context, queueParams *q.QueueParams, groupID string) error {
	return defaultConsumerGroupManager.Delete(ctx, queueParams, groupID)
}

// ListConsumerGroups returns consumer groups using the default manager.
func ListConsumerGroups(ctx context.Context, queueParams *q.QueueParams) ([]string, error) {
	return defaultConsumerGroupManager.List(ctx, queueParams)
}
