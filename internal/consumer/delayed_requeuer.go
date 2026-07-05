/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	mSchema "github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type DelayedRequeuer struct {
	queue      *q.QueueParams
	groupID    string
	consumerID string
	interval   time.Duration
	log        *slog.Logger
}

func NewDelayedRequeuer(queue *q.QueueParams, groupID, consumerID string) *DelayedRequeuer {
	return &DelayedRequeuer{
		queue:      queue,
		groupID:    groupID,
		consumerID: consumerID,
		interval:   5 * time.Second,
		log:        logger.New("consumer", "delayed-requeuer", consumerID, queue.Name()),
	}
}

func (dr *DelayedRequeuer) Run(ctx context.Context) {
	dr.log.Debug("starting delayed requeuer", "interval", dr.interval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				dr.log.Error("delayed requeuer panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(dr.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				dr.log.Debug("delayed requeuer stopped")
				return
			case <-ticker.C:
				dr.requeueDue(ctx)
			}
		}
	}()
}

func (dr *DelayedRequeuer) requeueDue(ctx context.Context) {
	qKey := keys.Queue{
		Namespace: dr.queue.NS(),
		Name:      dr.queue.Name(),
	}

	now := time.Now().UnixMilli()
	ids, err := redisClient.Client().ZRangeByScore(ctx, qKey.Delayed(), &redis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  99,
	}).Result()
	if err != nil {
		dr.log.Error("failed to fetch delayed messages", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	dr.log.Debug("found delayed messages", "count", len(ids))

	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := keys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "delayed message")
		if err != nil {
			dr.log.Debug("failed to load delayed message", "messageID", id, "error", err)
			continue
		}
		codec := internalMessage.NewEnvelopeCodec()
		env, err := codec.DecodeHash(ctx, hash)
		if err != nil {
			dr.log.Debug("failed to decode delayed message", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}

	if len(messages) == 0 {
		return
	}

	dr.enqueueDelayed(ctx, qKey, messages)
}

func (dr *DelayedRequeuer) enqueueDelayed(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope) {
	luaKeys := []string{
		qKey.Properties(),
		qKey.Delayed(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	}

	timestamp := time.Now().UnixMilli()

	argv := []interface{}{
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldDelayedMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldDeadLetteredMessagesCount.Key(),
		mSchema.MessageFieldStatus.Key(),
		msg.StatusPending.Int(),
		msg.StatusDeadLettered.Int(),
		mSchema.MessageFieldDeadLetteredAt.Key(),
		mSchema.MessageFieldLastRetriedAttemptAt.Key(),
		q.TypeLIFO.Int(),
		q.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		q.StateActive.Int(),
		q.StatePaused.Int(),
		q.StateStopped.Int(),
		q.StateLocked.Int(),
		timestamp,
	}

	requeuedCount := 0
	deadLetteredCount := 0

	for _, msg := range messages {
		msgID := msg.ID()
		msgKey := keys.System{}.Message(msgID)
		consumerGroupID := msg.ConsumerGroupID()

		priority := ""
		if msg.ProducibleMessage().HasPriority() {
			priority = fmt.Sprintf("%d", msg.ProducibleMessage().Priority().Int())
		}

		destKey := keys.Queue{Namespace: dr.queue.NS(), Name: dr.queue.Name()}

		luaKeys = append(luaKeys, msgKey, destKey.Pending(), destKey.Priority())

		argv = append(argv, msgID, priority, consumerGroupID)
		requeuedCount++
	}

	dr.log.Debug("requeuing delayed messages", "count", requeuedCount)

	reply, err := redisClient.Eval(ctx, scripts.RequeueDelayed, luaKeys, argv...)
	if err != nil {
		dr.log.Error("delayed requeue script failed", "error", err)
		return
	}

	switch v := reply.(type) {
	case int64:
		if v > 0 {
			dr.log.Info("delayed messages requeued", "processed", v)
		}
	case string:
		switch v {
		case "QUEUE_STOPPED":
			dr.log.Warn("queue stopped — cannot requeue delayed messages")
		case "QUEUE_LOCKED":
			dr.log.Warn("queue locked — cannot requeue delayed messages")
		case "QUEUE_INVALID_STATE":
			dr.log.Warn("queue in invalid state — cannot requeue delayed messages")
		default:
			dr.log.Error("unexpected script reply", "reply", v)
		}
	}

	if deadLetteredCount > 0 {
		dr.log.Warn("messages dead-lettered during delayed requeue", "count", deadLetteredCount)
	}
}
