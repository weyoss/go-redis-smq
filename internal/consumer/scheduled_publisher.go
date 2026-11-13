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
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/message/schema"
	internalProducer "github.com/weyoss/go-redis-smq/internal/producer"
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
	queue      *queue.Params
	groupID    string
	consumerID string
	interval   time.Duration
	wg         sync.WaitGroup // tracks the publish ticker goroutine
	log        *slog.Logger
}

func NewScheduledPublisher(queue *queue.Params, groupID, consumerID string) *ScheduledPublisher {
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
	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()
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

// Wait blocks until the background goroutine exits. Called by
// MessageHandler.beginShutdown after cancel, to guarantee no in-flight
// Redis operation remains when the process tears down the client via
// internalredis.Close.
//
// The goroutine exits as soon as its select observes ctx.Done() — but if
// it's mid-publishDue (a Redis ZRANGEBYSCORE plus per-message HGET plus
// the Lua EVAL), Wait blocks until that whole batch completes. That's the
// point: shutdown cannot proceed until the last Redis op is done.
func (sp *ScheduledPublisher) Wait() {
	sp.wg.Wait()
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
		codec := internalMessage.NewCodec()
		env, err := codec.DecodeEnvelope(ctx, hash)
		if err != nil {
			sp.log.Debug("failed to decode scheduled message", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}

	if len(messages) == 0 {
		return
	}

	// Group messages by consumer group ID.
	groups := make(map[string][]*internalMessage.Envelope)
	for _, msg := range messages {
		cgID := msg.ConsumerGroupID()
		groups[cgID] = append(groups[cgID], msg)
	}

	// Process each group separately with its own set of static keys.
	for cgID, msgs := range groups {
		sp.enqueueScheduled(ctx, qKey, msgs, cgID)
	}
}

// enqueueScheduled publishes the due scheduled messages for one consumer
// group (or for the queue-level pending list, if consumerGroupID is empty).
//
// The publish-scheduled.lua script returns { published_ids, dead_lettered_ids }:
//
//   - published_ids: messages the script actually moved into a pending
//     list. For simple scheduled messages, this is the message's own ID.
//     For repeating messages, this is the clone's ID (the parent stays
//     in the scheduled set for its next tick).
//
//   - dead_lettered_ids: messages the script moved to the dead-letter
//     list instead, because the target consumer group had been deleted
//     between when the message was scheduled and when its delivery was
//     due. A Pub/Sub message whose group is gone has nowhere to go —
//     dead-lettering makes the loss visible instead of orphaning it.
func (sp *ScheduledPublisher) enqueueScheduled(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope, consumerGroupID string) {
	pendingKey := qKey.Pending()
	priorityKey := qKey.Priority()
	if consumerGroupID != "" {
		pendingKey = qKey.PendingWithGroup(consumerGroupID)
		priorityKey = qKey.PriorityWithGroup(consumerGroupID)
	}

	luaKeys := []string{
		qKey.Properties(),
		pendingKey,
		qKey.Published(),
		priorityKey,
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
		"group", consumerGroupID,
	)

	reply, err := redisClient.Eval(ctx, scripts.PublishScheduled, luaKeys, argv...)
	if err != nil {
		sp.log.Error("scheduled publish script failed", "error", err)
		return
	}

	// String reply = script-level error (publish_message failure inside
	// the repeating-message path, or a malformed KEYS/ARGV shape). No
	// events fire — state is potentially partial and the error is a
	// single string, not per-message results.
	if replyStr, ok := reply.(string); ok && replyStr != "" {
		sp.log.Error("scheduled publish script returned error", "reply", replyStr)
		return
	}

	arr, ok := reply.([]interface{})
	if !ok {
		sp.log.Error("unexpected scheduled publish reply type",
			"type", fmt.Sprintf("%T", reply),
		)
		return
	}
	if len(arr) < 2 {
		sp.log.Error("unexpected scheduled publish reply shape",
			"len", len(arr))
		return
	}

	publishedIDs := toStringSlice(arr[0])
	if publishedIDs == nil {
		sp.log.Error("scheduled publish reply: published_ids is not a string array",
			"type", fmt.Sprintf("%T", arr[0]))
		return
	}

	deadLetteredIDs := toStringSlice(arr[1])
	if deadLetteredIDs == nil {
		sp.log.Error("scheduled publish reply: dead_lettered_ids is not a string array",
			"type", fmt.Sprintf("%T", arr[1]))
		return
	}

	if len(deadLetteredIDs) > 0 {
		sp.log.Warn("scheduled messages dead-lettered during publication "+
			"(consumer group no longer exists)",
			"count", len(deadLetteredIDs),
			"group", consumerGroupID,
		)
	}

	if len(publishedIDs) == 0 {
		return
	}

	queueParsed := queue.ParsedParams{
		QueueParams: *sp.queue,
		GroupID:     consumerGroupID,
	}
	for _, publishedID := range publishedIDs {
		internalProducer.PublishMessagePublished(ctx, publishedID, queueParsed, "")
	}
}

// toStringSlice extracts a []string from a Lua-replied table whose
// elements are all strings. Returns nil for non-table input, or for a
// table containing any non-string element. Callers treat nil as
// "malformed reply" and suppress event publishing — a wrong-shaped
// reply is a deployment error (stale script, contract drift), not a
// condition to handle gracefully with best-effort firing.
func toStringSlice(v interface{}) []string {
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil
		}
		out = append(out, s)
	}
	return out
}

// buildScheduledArgs builds the static ARGV for the PublishScheduled Lua script.
func buildScheduledArgs() []interface{} {
	return []interface{}{
		qSchema.Type.Key(),
		qSchema.MessagesCount.Key(),
		qSchema.PendingMessagesCount.Key(),
		qSchema.ScheduledMessagesCount.Key(),
		qSchema.DeadLetteredMessagesCount.Key(),
		queue.TypePriority.Int(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.OperationalState.Key(),
		qSchema.LockID.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		publicmessage.StatusPending.Int(),
		publicmessage.StatusScheduled.Int(),
		publicmessage.StatusDeadLettered.Int(),
		schema.ID.Key(),
		schema.Status.Key(),
		schema.Message.Key(),
		schema.ScheduledAt.Key(),
		schema.PublishedAt.Key(),
		schema.ProcessingStartedAt.Key(),
		schema.DeadLetteredAt.Key(),
		schema.AcknowledgedAt.Key(),
		schema.UnacknowledgedAt.Key(),
		schema.LastUnacknowledgedAt.Key(),
		schema.LastScheduledAt.Key(),
		schema.RequeuedAt.Key(),
		schema.RequeueCount.Key(),
		schema.LastRequeuedAt.Key(),
		schema.LastRetriedAttemptAt.Key(),
		schema.ScheduledCronFired.Key(),
		schema.Attempts.Key(),
		schema.ScheduledRepeatCount.Key(),
		schema.Expired.Key(),
		schema.EffectiveScheduledDelay.Key(),
		schema.ScheduledTimes.Key(),
		schema.ScheduledMessageParentID.Key(),
		schema.RequeuedMessageParentID.Key(),
		schema.LastProcessedAt.Key(),
	}
}
