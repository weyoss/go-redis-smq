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

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type ImmediateRequeuer struct {
	queue      *queue.Params
	groupID    string
	consumerID string
	interval   time.Duration
	log        *slog.Logger
}

func NewImmediateRequeuer(queue *queue.Params, groupID, consumerID string) *ImmediateRequeuer {
	return &ImmediateRequeuer{
		queue:      queue,
		groupID:    groupID,
		consumerID: consumerID,
		interval:   5 * time.Second,
		log:        logger.New("consumer", "immediate-requeuer", consumerID, queue.Name()),
	}
}

func (ir *ImmediateRequeuer) Run(ctx context.Context) {
	ir.log.Debug("starting immediate requeuer", "interval", ir.interval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ir.log.Error("immediate requeuer panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(ir.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				ir.log.Debug("immediate requeuer stopped")
				return
			case <-ticker.C:
				ir.requeue(ctx)
			}
		}
	}()
}

func (ir *ImmediateRequeuer) requeue(ctx context.Context) {
	qKey := keys.Queue{
		Namespace: ir.queue.NS(),
		Name:      ir.queue.Name(),
	}

	ids, err := redisClient.Client().LRange(ctx, qKey.Requeued(), 0, 99).Result()
	if err != nil {
		ir.log.Error("failed to fetch requeued messages", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	ir.log.Debug("found requeued messages", "count", len(ids))

	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := keys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "requeued message")
		if err != nil {
			ir.log.Debug("failed to load requeued message", "messageID", id, "error", err)
			continue
		}
		codec := internalMessage.NewEnvelopeCodec()
		env, err := codec.DecodeHash(ctx, hash)
		if err != nil {
			ir.log.Debug("failed to decode requeued message", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}

	if len(messages) == 0 {
		return
	}

	ir.requeueMessages(ctx, qKey, messages)
}

func (ir *ImmediateRequeuer) requeueMessages(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope) {
	luaKeys := []string{
		qKey.Properties(),
		qKey.Requeued(),
		qKey.Delayed(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	}

	timestamp := time.Now().UnixMilli()

	argv := []interface{}{
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldRequeuedMessagesCount.Key(),
		qSchema.QueueFieldDelayedMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldDeadLetteredMessagesCount.Key(),
		internalMessage.MessageFieldStatus.Key(),
		publicmessage.StatusPending.Int(),
		publicmessage.StatusDeadLettered.Int(),
		internalMessage.MessageFieldDeadLetteredAt.Key(),
		publicmessage.StatusUnackDelaying.Int(),
		internalMessage.MessageFieldLastRetriedAttemptAt.Key(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		timestamp,
	}

	pendingCount := 0
	delayedCount := 0
	deadLetteredCount := 0

	for _, msg := range messages {
		msgID := msg.ID()
		msgKey := keys.System{}.Message(msgID)
		consumerGroupID := msg.ConsumerGroupID()

		priority := ""
		if msg.ProducibleMessage().HasPriority() {
			priority = fmt.Sprintf("%d", msg.ProducibleMessage().Priority().Int())
		}

		retryDelay := msg.ProducibleMessage().RetryDelay()
		delayedTimestamp := int64(0)
		if retryDelay > 0 {
			delayedTimestamp = timestamp + retryDelay.Milliseconds()
			delayedCount++
		} else {
			pendingCount++
		}

		destKey := keys.Queue{Namespace: ir.queue.NS(), Name: ir.queue.Name()}
		pendingKey := destKey.Pending()
		priorityKey := destKey.Priority()
		if consumerGroupID != "" {
			pendingKey = destKey.PendingWithGroup(consumerGroupID)
			priorityKey = destKey.PriorityWithGroup(consumerGroupID)
		}
		luaKeys = append(luaKeys, msgKey, pendingKey, priorityKey)

		argv = append(argv,
			msgID,
			priority,
			fmt.Sprintf("%d", retryDelay.Milliseconds()),
			fmt.Sprintf("%d", delayedTimestamp),
			consumerGroupID,
		)
	}

	ir.log.Debug("requeuing messages",
		"total", len(messages),
		"pending", pendingCount,
		"delayed", delayedCount,
	)

	reply, err := redisClient.Eval(ctx, scripts.RequeueImmediate, luaKeys, argv...)
	if err != nil {
		ir.log.Error("immediate requeue script failed", "error", err)
		return
	}

	switch v := reply.(type) {
	case int64:
		if v > 0 {
			ir.log.Info("messages requeued", "processed", v)
		}
	case string:
		switch v {
		case "QUEUE_STOPPED":
			ir.log.Warn("queue stopped — cannot requeue messages")
		case "QUEUE_LOCKED":
			ir.log.Warn("queue locked — cannot requeue messages")
		case "QUEUE_INVALID_STATE":
			ir.log.Warn("queue in invalid state — cannot requeue messages")
		default:
			ir.log.Error("unexpected script reply", "reply", v)
		}
	}

	if deadLetteredCount > 0 {
		ir.log.Warn("messages dead-lettered during requeue", "count", deadLetteredCount)
	}
}
