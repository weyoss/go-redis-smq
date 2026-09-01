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
	"github.com/weyoss/go-redis-smq/internal/ratelimit"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

const maxQueueStateHistorySize = 100

type Store struct {
	codec          *Codec
	rateLimitStore *ratelimit.Store
}

func NewStore(codec *Codec) *Store {
	if codec == nil {
		codec = NewCodec()
	}
	return &Store{
		codec:          codec,
		rateLimitStore: ratelimit.NewStore(),
	}
}

func (s *Store) Save(
	ctx context.Context,
	queueParams *publicqueue.Params,
	queueType publicqueue.Type,
	deliveryModel publicqueue.DeliveryModel,
) error {
	return s.SaveWithRateLimit(ctx, queueParams, queueType, deliveryModel, nil)
}

func (s *Store) SaveWithRateLimit(
	ctx context.Context,
	queueParams *publicqueue.Params,
	queueType publicqueue.Type,
	deliveryModel publicqueue.DeliveryModel,
	rateLimit *publicqueue.RateLimitParams,
) error {
	if queueParams == nil {
		return publicqueue.ErrNameRequired
	}

	now := time.Now().UnixMilli()
	namespace := queueParams.NS()
	queueName := queueParams.Name()

	nsKeys := keys.Namespace{Name: namespace}
	systemKeys := keys.System{}
	queueKeys := keys.Queue{Namespace: namespace, Name: queueName}

	queueParamsJSON, err := s.codec.EncodeParams(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("encode queue params: %w", err)
	}

	initialTransition := publicqueue.StateTransition{
		From:      nil,
		To:        publicqueue.StateActive,
		Reason:    publicqueue.TransitionReason(publicqueue.ReasonSystemInit),
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
		rateLimitJSON, _ = s.rateLimitStore.Codec().EncodeJSON(ctx, rateLimit)
	}

	argv := []interface{}{
		namespace,
		queueParamsJSON,
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
		publicqueue.StateActive.Int(),
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
		queueEvents.PublishCreated(ctx, *queueParams, publicqueue.Props{
			Type:          queueType,
			DeliveryModel: deliveryModel,
			RateLimit:     rateLimit,
		})
		return nil
	case "QUEUE_EXISTS":
		return publicqueue.ErrAlreadyExists
	default:
		return fmt.Errorf("create queue: unexpected script reply: %s", replyStr)
	}
}

func (s *Store) SetRateLimit(ctx context.Context, queueParams *publicqueue.Params, rateLimit *publicqueue.RateLimitParams) error {
	return s.rateLimitStore.Set(ctx, queueParams, rateLimit)
}

func (s *Store) ClearRateLimit(ctx context.Context, queueParams *publicqueue.Params) error {
	return s.rateLimitStore.Clear(ctx, queueParams)
}

func (s *Store) GetRateLimit(ctx context.Context, queueParams *publicqueue.Params) (*publicqueue.RateLimitParams, error) {
	return s.rateLimitStore.Get(ctx, queueParams)
}

func (s *Store) Load(ctx context.Context, queueParams *publicqueue.Params) (*publicqueue.Props, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}.Properties()

	hash, err := redisClient.LoadHash(ctx, key, "queue properties")
	if err != nil {
		return nil, err
	}

	props, err := s.codec.DecodeProps(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("load queue: decode: %w", err)
	}

	return props, nil
}

func (s *Store) Exists(ctx context.Context, queueParams *publicqueue.Params) (bool, error) {
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

func (s *Store) Delete(ctx context.Context, queueParams *publicqueue.Params) error {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	// Consumers is a Redis hash; use HKeys.
	consumerIDs, err := redisClient.Client().HKeys(ctx, key.Consumers()).Result()
	if err != nil {
		return fmt.Errorf("delete queue: get consumers: %w", err)
	}

	// ConsumerGroups is a Redis set; use SMembers.
	consumerGroups, err := redisClient.Client().SMembers(ctx, key.ConsumerGroups()).Result()
	if err != nil {
		return fmt.Errorf("delete queue: get consumer groups: %w", err)
	}

	// ProcessingQueues is a Redis hash; use HKeys.
	processingQueues, err := redisClient.Client().HKeys(ctx, key.ProcessingQueues()).Result()
	if err != nil {
		return fmt.Errorf("delete queue: get processing queues: %w", err)
	}

	heartbeatKeys := make([]string, len(consumerIDs))
	for i, cid := range consumerIDs {
		heartbeatKeys[i] = keys.System{}.ConsumerHeartbeat(cid)
	}

	consumerGroupKeys := make([]string, 0, len(consumerGroups)*2)
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
		publicqueue.StateLocked.Int(),
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
		return publicqueue.ErrLocked
	case "QUEUE_NOT_FOUND":
		return publicqueue.ErrNotFound
	case "QUEUE_NOT_EMPTY":
		return publicqueue.ErrQueueNotEmpty
	case "QUEUE_HAS_ACTIVE_CONSUMERS":
		return publicqueue.ErrQueueHasActiveConsumers
	case "QUEUE_HAS_BOUND_EXCHANGE":
		return publicqueue.ErrQueueHasBoundExchanges
	case "CONSUMER_SET_MISMATCH":
		return publicqueue.ErrConsumerSetMismatch
	default:
		return fmt.Errorf("delete queue: unexpected script reply: %s", replyStr)
	}
}
