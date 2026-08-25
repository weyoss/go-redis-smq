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
	"fmt"

	queueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

type ConsumerGroupStore struct{}

func NewConsumerGroupStore() *ConsumerGroupStore {
	return &ConsumerGroupStore{}
}

func (cgs *ConsumerGroupStore) Save(ctx context.Context, queueParams *publicqueue.QueueParams, groupID string) (int64, error) {
	if _, err := keys.ValidateKey(groupID); err != nil {
		return 0, fmt.Errorf("invalid consumer group ID: %w", err)
	}

	props, err := NewStore(nil).Load(ctx, queueParams)
	if err != nil {
		return 0, err
	}

	if props.DeliveryModel != publicqueue.DeliveryPubSub {
		return 0, publicqueue.ErrConsumerGroupsNotSupported
	}

	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	result, err := redisClient.Client().SAdd(ctx, qKey.ConsumerGroups(), groupID).Result()
	if err != nil {
		return 0, fmt.Errorf("save consumer group: %w", err)
	}

	if result == 1 {
		queueEvents.PublishConsumerGroupCreated(ctx, *queueParams, groupID)
	}

	return result, nil
}

func (cgs *ConsumerGroupStore) Delete(ctx context.Context, queueParams *publicqueue.QueueParams, groupID string) error {
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	consumerGroupKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	luaKeys := []string{
		qKey.ConsumerGroups(),
		qKey.Pending(),
		qKey.Priority(),
		qKey.Properties(),
		consumerGroupKey.ConsumerGroupMembers(groupID),
	}

	argv := []interface{}{
		schema.QueueFieldType.Key(),
		publicqueue.TypePriority.Int(),
		schema.QueueFieldDeliveryModel.Key(),
		publicqueue.DeliveryPubSub.Int(),
		groupID,
		schema.QueueFieldOperationalState.Key(),
		publicqueue.StateLocked.String(),
		schema.QueueFieldLockID.Key(),
		"",
	}

	reply, err := redisClient.Eval(ctx, scripts.DeleteConsumerGroup, luaKeys, argv...)
	if err != nil {
		return fmt.Errorf("delete consumer group: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		return err
	}

	switch replyStr {
	case "OK":
		queueEvents.PublishConsumerGroupDeleted(ctx, *queueParams, groupID)
		return nil
	case "QUEUE_LOCKED":
		return publicqueue.ErrLocked
	case "QUEUE_NOT_FOUND":
		return publicqueue.ErrNotFound
	case "CONSUMER_GROUPS_NOT_SUPPORTED":
		return publicqueue.ErrConsumerGroupsNotSupported
	case "CONSUMER_GROUP_NOT_EMPTY":
		return publicqueue.ErrConsumerGroupNotEmpty
	case "CONSUMER_GROUP_HAS_ACTIVE_CONSUMERS":
		return publicqueue.ErrConsumerGroupHasActiveConsumers
	default:
		return fmt.Errorf("delete consumer group: unexpected script reply: %s", replyStr)
	}
}

func (cgs *ConsumerGroupStore) List(ctx context.Context, queueParams *publicqueue.QueueParams) ([]string, error) {
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	groups, err := redisClient.LoadSetMembers(ctx, qKey.ConsumerGroups(), "consumer groups")
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}

	return groups, nil
}
