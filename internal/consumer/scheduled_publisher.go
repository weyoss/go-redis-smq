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
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type ScheduledPublisher struct {
	queue      *queue.QueueParams
	groupID    string
	consumerID string
	interval   time.Duration
	log        *slog.Logger
}

func NewScheduledPublisher(queue *queue.QueueParams, groupID, consumerID string) *ScheduledPublisher {
	return &ScheduledPublisher{
		queue:      queue,
		groupID:    groupID,
		consumerID: consumerID,
		interval:   5 * time.Second,
		log:        logger.New("consumer", "scheduled-publisher", consumerID, queue.Name()),
	}
}

func (sp *ScheduledPublisher) Run(ctx context.Context) {
	sp.log.Debug("starting scheduled publisher", "interval", sp.interval)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				sp.log.Error("scheduled publisher panicked", "panic", r)
			}
		}()
		ticker := time.NewTicker(sp.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				sp.log.Debug("scheduled publisher stopped")
				return
			case <-ticker.C:
				sp.publishDue(ctx)
			}
		}
	}()
}

func (sp *ScheduledPublisher) publishDue(ctx context.Context) {
	qKey := keys.Queue{
		Namespace: sp.queue.NS(),
		Name:      sp.queue.Name(),
	}

	now := time.Now().UnixMilli()
	ids, err := redisClient.Client().ZRangeByScore(ctx, qKey.Scheduled(), &redis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", now),
		Offset: 0,
		Count:  99,
	}).Result()
	if err != nil {
		sp.log.Error("failed to fetch scheduled messages", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	sp.log.Debug("found due scheduled messages", "count", len(ids))

	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := keys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "scheduled message")
		if err != nil {
			sp.log.Debug("failed to load scheduled message", "messageID", id, "error", err)
			continue
		}
		codec := internalMessage.NewEnvelopeCodec()
		env, err := codec.DecodeHash(ctx, hash)
		if err != nil {
			sp.log.Debug("failed to decode scheduled message", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}

	if len(messages) == 0 {
		return
	}

	sp.enqueueScheduled(ctx, qKey, messages)
}

func (sp *ScheduledPublisher) enqueueScheduled(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope) {
	luaKeys := []string{
		qKey.Properties(),
		qKey.Pending(),
		qKey.Published(),
		qKey.Priority(),
		qKey.Scheduled(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	}

	argv := buildScheduledArgs()

	simpleCount := 0
	repeatCount := 0

	for _, msg := range messages {
		msgID := msg.ID()
		state := msg.MessageState()
		now := time.Now().UnixMilli()

		nextSchedule := msg.NextScheduledTimestamp()
		isPeriodic := msg.IsPeriodic()
		consumerGroupID := msg.ConsumerGroupID()
		priority := ""
		if msg.ProducibleMessage().HasPriority() {
			priority = fmt.Sprintf("%d", msg.ProducibleMessage().Priority().Int())
		}

		var newMessageID, newMessageJSON, newKeyMessage string
		var newMessagePublishedAt int64

		if nextSchedule > 0 && isPeriodic {
			clone := internalMessage.CloneMessage(msg)
			clone.ProducibleMessage().ResetScheduledParams()
			cloneState := clone.MessageState()
			cloneState.SetPublishedAt(now)
			cloneState.SetScheduledMessageParentID(msgID)

			newMessageID = clone.ID()
			newKeyMessage = keys.System{}.Message(newMessageID)

			newMessageBytes, _ := json.Marshal(clone.ToTransferable())
			newMessageJSON = string(newMessageBytes)
			newMessagePublishedAt = now

			state.SetLastScheduledAt(now)
			state.IncrScheduledTimes()
			repeatCount++
		} else {
			simpleCount++
		}

		scheduledMsgKey := keys.System{}.Message(msgID)

		luaKeys = append(luaKeys, newKeyMessage, scheduledMsgKey)

		argv = append(argv,
			newMessageID,
			newMessageJSON,
			priority,
			newMessagePublishedAt,
			msgID,
			nextSchedule,
			util.OrEmptyInt64(state.LastScheduledAt()),
			state.ScheduledTimes(),
			util.OrEmptyInt64(state.PublishedAt()),
			util.BoolToInt(state.ScheduledCronFired()),
			state.ScheduledRepeatCount(),
			state.EffectiveScheduledDelay(),
			consumerGroupID,
			now,
		)
	}

	sp.log.Debug("publishing scheduled messages",
		"total", len(messages),
		"simple", simpleCount,
		"repeating", repeatCount,
	)

	reply, err := redisClient.Eval(ctx, scripts.PublishScheduled, luaKeys, argv...)
	if err != nil {
		sp.log.Error("scheduled publish script failed", "error", err)
		return
	}

	if replyStr, ok := reply.(string); ok && replyStr != "" {
		sp.log.Error("scheduled publish script returned error", "reply", replyStr)
		return
	}

	if n, ok := reply.(int64); ok && n > 0 {
		sp.log.Info("scheduled messages published", "count", n)
	}
}

// buildScheduledArgs builds the static ARGV for the PublishScheduled Lua script.
// This script needs 14 queue properties + 3 status constants + 24 message property keys.
func buildScheduledArgs() []interface{} {
	return []interface{}{
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldScheduledMessagesCount.Key(),
		qSchema.QueueFieldDeadLetteredMessagesCount.Key(),
		queue.TypePriority.Int(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		qSchema.QueueFieldLockID.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		publicmessage.StatusPending.Int(),
		publicmessage.StatusScheduled.Int(),
		publicmessage.StatusDeadLettered.Int(),
		internalMessage.MessageFieldID.Key(),
		internalMessage.MessageFieldStatus.Key(),
		internalMessage.MessageFieldMessage.Key(),
		internalMessage.MessageFieldScheduledAt.Key(),
		internalMessage.MessageFieldPublishedAt.Key(),
		internalMessage.MessageFieldProcessingStartedAt.Key(),
		internalMessage.MessageFieldDeadLetteredAt.Key(),
		internalMessage.MessageFieldAcknowledgedAt.Key(),
		internalMessage.MessageFieldUnacknowledgedAt.Key(),
		internalMessage.MessageFieldLastUnacknowledgedAt.Key(),
		internalMessage.MessageFieldLastScheduledAt.Key(),
		internalMessage.MessageFieldRequeuedAt.Key(),
		internalMessage.MessageFieldRequeueCount.Key(),
		internalMessage.MessageFieldLastRequeuedAt.Key(),
		internalMessage.MessageFieldLastRetriedAttemptAt.Key(),
		internalMessage.MessageFieldScheduledCronFired.Key(),
		internalMessage.MessageFieldAttempts.Key(),
		internalMessage.MessageFieldScheduledRepeatCount.Key(),
		internalMessage.MessageFieldExpired.Key(),
		internalMessage.MessageFieldEffectiveScheduledDelay.Key(),
		internalMessage.MessageFieldScheduledTimes.Key(),
		internalMessage.MessageFieldScheduledMessageParentID.Key(),
		internalMessage.MessageFieldRequeuedMessageParentID.Key(),
		internalMessage.MessageFieldLastProcessedAt.Key(),
	}
}
