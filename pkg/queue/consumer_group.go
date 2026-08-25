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
)

// ConsumerGroupManager is the public interface for managing consumer groups
// on Pub/Sub queues.
//
// Consumer groups enable the Pub/Sub delivery model where each message is
// delivered to all groups, but only to a single consumer within each group.
// The concrete implementation is provided by the root redissmq package.
type ConsumerGroupManager interface {
	// Save creates a consumer group for a queue.
	// It returns 1 if the group was newly created, or 0 if it already existed.
	Save(ctx context.Context, queueParams *QueueParams, groupID string) (int64, error)

	// Delete removes a consumer group from a queue.
	Delete(ctx context.Context, queueParams *QueueParams, groupID string) error

	// List returns all consumer group IDs for a queue.
	List(ctx context.Context, queueParams *QueueParams) ([]string, error)
}
