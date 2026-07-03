/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"

	internalQueueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	queueEvents "github.com/weyoss/go-redis-smq/pkg/queue/events"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type PubSubTargetResolver struct {
	mu         sync.RWMutex
	targets    map[string][]string
	subscribed bool
	log        *slog.Logger
}

func NewPubSubTargetResolver(producerID string) *PubSubTargetResolver {
	return &PubSubTargetResolver{
		targets: make(map[string][]string),
		log:     logger.New("producer", "pubsub-resolver", producerID),
	}
}

func (r *PubSubTargetResolver) onQueueCreated(p internalQueueEvents.CreatedPayload) {
	if p.Properties.DeliveryModel == q.DeliveryPubSub {
		key := p.Queue.String()
		r.mu.Lock()
		if _, exists := r.targets[key]; !exists {
			r.targets[key] = []string{}
			r.log.Debug("new pub/sub queue registered", "queue", key)
		}
		r.mu.Unlock()
	}
}

func (r *PubSubTargetResolver) onQueueDeleted(p internalQueueEvents.DeletedPayload) {
	key := p.Queue.String()
	r.mu.Lock()
	delete(r.targets, key)
	r.mu.Unlock()
	r.log.Debug("pub/sub queue removed", "queue", key)
}

func (r *PubSubTargetResolver) onConsumerGroupCreated(p internalQueueEvents.ConsumerGroupCreatedPayload) {
	r.Add(&p.Queue, p.GroupID)
	r.log.Debug("consumer group added",
		"queue", p.Queue.String(),
		"group", p.GroupID,
	)
}

func (r *PubSubTargetResolver) onConsumerGroupDeleted(p internalQueueEvents.ConsumerGroupDeletedPayload) {
	r.Remove(&p.Queue, p.GroupID)
	r.log.Debug("consumer group removed",
		"queue", p.Queue.String(),
		"group", p.GroupID,
	)
}

func (r *PubSubTargetResolver) Load(ctx context.Context) error {
	queues, err := redis.LoadSetMembers(ctx, keys.System{}.AllQueues(), "all queues")
	if err != nil {
		r.log.Error("failed to load queues", "error", err)
		return err
	}

	r.log.Debug("loading pub/sub targets", "totalQueues", len(queues))

	pubSubCount := 0
	for _, member := range queues {
		var qp q.QueueParams
		if err := json.Unmarshal([]byte(member), &qp); err != nil {
			continue
		}
		if qp.Name() == "" || qp.NS() == "" {
			continue
		}

		propsKey := keys.Queue{Namespace: qp.NS(), Name: qp.Name()}.Properties()
		hash, err := redis.LoadHash(ctx, propsKey, "queue properties")
		if err != nil {
			r.log.Debug("failed to load queue properties", "queue", qp.String(), "error", err)
			continue
		}

		deliveryModelStr, ok := hash["3"]
		if !ok {
			continue
		}

		deliveryModel, _ := strconv.Atoi(deliveryModelStr)
		if q.DeliveryModel(deliveryModel) != q.DeliveryPubSub {
			continue
		}

		cgKey := keys.Queue{Namespace: qp.NS(), Name: qp.Name()}.ConsumerGroups()
		groups, err := redis.LoadSetMembers(ctx, cgKey, "consumer groups")
		if err != nil {
			r.log.Debug("failed to load consumer groups", "queue", qp.String(), "error", err)
			continue
		}

		r.mu.Lock()
		r.targets[qp.String()] = groups
		r.mu.Unlock()
		pubSubCount++
	}

	r.log.Info("pub/sub targets loaded",
		"pubSubQueues", pubSubCount,
	)

	if !r.subscribed {
		queueEvents.SubscribeCreated(r.onQueueCreated)
		queueEvents.SubscribeDeleted(r.onQueueDeleted)
		queueEvents.SubscribeConsumerGroupCreated(r.onConsumerGroupCreated)
		queueEvents.SubscribeConsumerGroupDeleted(r.onConsumerGroupDeleted)
		r.subscribed = true
		r.log.Debug("subscribed to queue events")
	}

	return nil
}

func (r *PubSubTargetResolver) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = make(map[string][]string)
	r.log.Debug("pub/sub targets cleared")
}

func (r *PubSubTargetResolver) Resolve(queueParams *q.QueueParams) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.targets[queueParams.String()]
}

func (r *PubSubTargetResolver) Add(queueParams *q.QueueParams, groupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := queueParams.String()
	for _, g := range r.targets[key] {
		if g == groupID {
			return
		}
	}
	r.targets[key] = append(r.targets[key], groupID)
}

func (r *PubSubTargetResolver) Remove(queueParams *q.QueueParams, groupID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := queueParams.String()
	groups := r.targets[key]
	for i, g := range groups {
		if g == groupID {
			r.targets[key] = append(groups[:i], groups[i+1:]...)
			return
		}
	}
}
