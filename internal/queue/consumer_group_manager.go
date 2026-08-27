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

// ConsumerGroupManager is the concrete implementation of the public consumer
// group manager interface. It delegates all operations to the internal
// ConsumerGroupStore.
type ConsumerGroupManager struct {
	store *ConsumerGroupStore
}

// NewConsumerGroupManager creates a new concrete consumer group manager that
// satisfies the public queue.ConsumerGroupManager interface.
func NewConsumerGroupManager() publicqueue.ConsumerGroupManager {
	return &ConsumerGroupManager{store: NewConsumerGroupStore()}
}

func (cgm *ConsumerGroupManager) Save(ctx context.Context, queueParams *publicqueue.Params, groupID string) (int64, error) {
	return cgm.store.Save(ctx, queueParams, groupID)
}

func (cgm *ConsumerGroupManager) Delete(ctx context.Context, queueParams *publicqueue.Params, groupID string) error {
	return cgm.store.Delete(ctx, queueParams, groupID)
}

func (cgm *ConsumerGroupManager) List(ctx context.Context, queueParams *publicqueue.Params) ([]string, error) {
	return cgm.store.List(ctx, queueParams)
}
