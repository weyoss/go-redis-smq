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
	"encoding/json"
	"fmt"
	"time"

	queueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/rate_limit"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

const maxQueueStateHistorySize = 100

type Store struct {
	codecs         *Codecs
	rateLimitStore *rate_limit.Store
}

func NewStore(codecs *Codecs) *Store {
	if codecs == nil {
		codecs = DefaultCodecs()
	}
	return &Store{
		codecs:         codecs,
		rateLimitStore: rate_limit.NewStore(),
	}
}

func (s *Store) Save(
	ctx context.Context,
	queueParams *q.QueueParams,
	queueType q.QueueType,
	deliveryModel q.DeliveryModel,
) error {
	return s.SaveWithRateLimit(ctx, queueParams, queueType, deliveryModel, nil)
}

func (s *Store) SaveWithRateLimit(
	ctx context.Context,
	queueParams *q.QueueParams,
	queueType q.QueueType,
	deliveryModel q.DeliveryModel,
	rateLimit *q.RateLimitParams,
) error {
	if queueParams == nil {
		return q.ErrNameRequired
	}

	now := time.Now().UnixMilli()
	namespace := queueParams.NS()
	queueName := queueParams.Name()

	nsKeys := keys.Namespace{Name: namespace}
	systemKeys := keys.System{}
	queueKeys := keys.Queue{Namespace: namespace, Name: queueName}

	queueParamsJSON, err := json.Marshal(queueParams)
	if err != nil {
		return fmt.Errorf("marshal queue params: %w", err)
	}

	initialTransition := q.StateTransition{
		From:      nil,
		To:        q.StateActive,
		Reason:    q.QueueStateTransitionReason(q.ReasonSystemInit),
		Timestamp: now,
		Metadata: map[string]interface{}{
			"queueType":     queueType.Int(),
			"deliveryModel": deliveryModel.Int(),
		},
	}
	initialTransitionJSON, err := json.Marshal(initialTransition)
	if err != nil {
		return fmt.Errorf("marshal initial transition: %w", err)
	}

	luaKeys := []string{
		systemKeys.AllNamespaces(),
		nsKeys.Queues(),
		systemKeys.AllQueues(),
		queueKeys.Properties(),
		queueKeys.StateHistory(),
	}

	rateLimitJSON := ""
	if rateLimit != nil {
		rateLimitJSON, _ = s.rateLimitStore.Codec().EncodeJSON(context.Background(), rateLimit)
	}

	argv := []interface{}{
		namespace,
		string(queueParamsJSON),
		schema.QueueFieldType.Key(),
		queueType.Int(),
		schema.QueueFieldDeliveryModel.Key(),
		deliveryModel.Int(),
		schema.QueueFieldRateLimit.Key(),
		rateLimitJSON,
		schema.QueueFieldMessagesCount.Key(),
		schema.QueueFieldAcknowledgedMessagesCount.Key(),
		schema.QueueFieldDeadLetteredMessagesCount.Key(),
		schema.QueueFieldPendingMessagesCount.Key(),
		schema.QueueFieldScheduledMessagesCount.Key(),
		schema.QueueFieldProcessingMessagesCount.Key(),
		schema.QueueFieldDelayedMessagesCount.Key(),
		schema.QueueFieldRequeuedMessagesCount.Key(),
		schema.QueueFieldOperationalState.Key(),
		q.StateActive.Int(),
		maxQueueStateHistorySize,
		schema.QueueFieldLastStateChangeAt.Key(),
		fmt.Sprintf("%d", now),
		schema.QueueFieldLockID.Key(),
		string(initialTransitionJSON),
	}

	reply, err := redisClient.Eval(ctx, scripts.CreateQueue, luaKeys, argv...)
	if err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		return err
	}

	switch replyStr {
	case "OK":
		queueEvents.PublishCreated(ctx, *queueParams, q.QueueProps{
			Type:          queueType,
			DeliveryModel: deliveryModel,
			RateLimit:     rateLimit,
		})
		return nil
	case "QUEUE_EXISTS":
		return q.ErrAlreadyExists
	default:
		return fmt.Errorf("create queue: unexpected script reply: %s", replyStr)
	}
}

func (s *Store) SetRateLimit(ctx context.Context, queueParams *q.QueueParams, rateLimit *q.RateLimitParams) error {
	return s.rateLimitStore.Set(ctx, queueParams, rateLimit)
}

func (s *Store) ClearRateLimit(ctx context.Context, queueParams *q.QueueParams) error {
	return s.rateLimitStore.Clear(ctx, queueParams)
}

func (s *Store) GetRateLimit(ctx context.Context, queueParams *q.QueueParams) (*q.RateLimitParams, error) {
	return s.rateLimitStore.Get(ctx, queueParams)
}

func (s *Store) Load(ctx context.Context, queueParams *q.QueueParams) (*q.QueueProps, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}.Properties()

	hash, err := redisClient.LoadHash(ctx, key, "queue properties")
	if err != nil {
		return nil, err
	}

	props, err := s.codecs.Props.DecodeHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("load queue: decode: %w", err)
	}

	return props, nil
}

func (s *Store) Exists(ctx context.Context, queueParams *q.QueueParams) (bool, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}.Properties()

	count, err := redisClient.Client().Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("check queue exists: %w", err)
	}
	return count > 0, nil
}

func (s *Store) Delete(ctx context.Context, queueParams *q.QueueParams) error {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	consumerIDs, err := redisClient.LoadSetMembers(ctx, key.Consumers(), "consumers")
	if err != nil {
		return fmt.Errorf("delete queue: get consumers: %w", err)
	}

	consumerGroups, err := redisClient.LoadSetMembers(ctx, key.ConsumerGroups(), "consumer groups")
	if err != nil {
		return fmt.Errorf("delete queue: get consumer groups: %w", err)
	}

	processingQueues, err := redisClient.LoadSetMembers(ctx, key.ProcessingQueues(), "processing queues")
	if err != nil {
		return fmt.Errorf("delete queue: get processing queues: %w", err)
	}

	heartbeatKeys := make([]string, len(consumerIDs))
	for i, cid := range consumerIDs {
		heartbeatKeys[i] = keys.System{}.ConsumerHeartbeat(cid)
	}

	var consumerGroupKeys []string
	for _, groupID := range consumerGroups {
		gKey := keys.Queue{
			Namespace: queueParams.NS(),
			Name:      queueParams.Name(),
		}
		consumerGroupKeys = append(consumerGroupKeys,
			gKey.PendingWithGroup(groupID),
			gKey.PriorityWithGroup(groupID),
		)
	}

	keysToDelete := []string{
		key.Properties(),
		key.Pending(),
		key.DeadLetter(),
		key.ProcessingQueues(),
		key.Priority(),
		key.Acknowledged(),
		key.Consumers(),
		key.RateLimit(),
		key.Scheduled(),
		key.Delayed(),
		key.Requeued(),
		key.Published(),
		key.ConsumerGroups(),
		key.WorkersLock(),
		key.ExchangeBindings(),
	}
	keysToDelete = append(keysToDelete, consumerGroupKeys...)
	keysToDelete = append(keysToDelete, processingQueues...)

	seen := make(map[string]bool)
	var uniqueKeys []string
	for _, k := range keysToDelete {
		if !seen[k] {
			seen[k] = true
			uniqueKeys = append(uniqueKeys, k)
		}
	}

	luaKeys := []string{
		keys.System{}.AllQueues(),
		keys.Namespace{Name: queueParams.NS()}.Queues(),
		key.Properties(),
		key.ExchangeBindings(),
		key.Consumers(),
	}
	luaKeys = append(luaKeys, heartbeatKeys...)
	for _, k := range uniqueKeys {
		if k != key.Properties() && k != key.ExchangeBindings() && k != key.Consumers() {
			luaKeys = append(luaKeys, k)
		}
	}

	queueParamsJSON, _ := json.Marshal(queueParams)
	argv := []interface{}{
		string(queueParamsJSON),
		schema.QueueFieldMessagesCount.Key(),
		len(heartbeatKeys),
		schema.QueueFieldOperationalState.Key(),
		q.StateLocked.Int(),
		schema.QueueFieldLockID.Key(),
		"",
	}
	for _, cid := range consumerIDs {
		argv = append(argv, cid)
	}

	reply, err := redisClient.Eval(ctx, scripts.DeleteQueue, luaKeys, argv...)
	if err != nil {
		return fmt.Errorf("delete queue: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		return err
	}

	switch replyStr {
	case "OK":
		queueEvents.PublishDeleted(ctx, *queueParams)
		return nil
	case "QUEUE_LOCKED":
		return q.ErrLocked
	case "QUEUE_NOT_FOUND":
		return q.ErrNotFound
	case "QUEUE_NOT_EMPTY":
		return q.ErrQueueNotEmpty
	case "QUEUE_HAS_ACTIVE_CONSUMERS":
		return q.ErrQueueHasActiveConsumers
	case "QUEUE_HAS_BOUND_EXCHANGE":
		return q.ErrQueueHasBoundExchanges
	case "CONSUMER_SET_MISMATCH":
		return q.ErrConsumerSetMismatch
	default:
		return fmt.Errorf("delete queue: unexpected script reply: %s", replyStr)
	}
}
