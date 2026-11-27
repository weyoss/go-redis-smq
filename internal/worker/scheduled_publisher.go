/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// scheduledPublisher periodically moves due scheduled messages into the
// pending queue.
//
// Queue-scoped worker: spawned by Coordinator for every queue whose
// WorkersLock this process holds. Runs until ctx is cancelled (by lease
// loss, queue removal, or coordinator shutdown).
//
// Idempotence: case 2 of publish-scheduled.lua (repeating messages) is
// safe under concurrent execution only because of the ZREM gate added to
// the script. Without that gate, two executors for the same queue would
// generate two different clone IDs and publish two clones. See the
// script's docstring for the split-brain rationale.
type scheduledPublisher struct {
	queue    *queue.Params
	interval time.Duration
	log      *slog.Logger
}

// newScheduledPublisher constructs the worker for the given queue.
//
// No consumer ID: the scheduler is not tied to a consumer instance. Any
// message it moves to pending is available to every consumer subscribed
// to that queue (or to the target consumer group, for Pub/Sub).
func newScheduledPublisher(qp *queue.Params, interval time.Duration) *scheduledPublisher {
	return &scheduledPublisher{
		queue:    qp,
		interval: interval,
		log:      logger.New("worker", "scheduled-publisher", qp.Name()),
	}
}

// Run blocks until ctx is cancelled. Spawned as a goroutine by
// queueWorkers.trySpawnWorkers; the coordinator's per-queue wg tracks
// this Run call, so wg.Wait() waits for the ticker loop (including any
// in-flight publishDue) to exit.
//
// Recover is local: a panic in publishDue kills this worker's goroutine
// but does not crash the process. The coordinator's reconcile tick will
// re-elect on the next cycle (this worker's ctx stays cancelled because
// nothing re-spawns it from within the panic handler; the next reconcile
// will either re-acquire and re-spawn, or leave it to whoever currently
// holds the lease).
func (sp *scheduledPublisher) Run(ctx context.Context) {
	sp.log.Debug("starting scheduled publisher", "interval", sp.interval)
	defer sp.log.Debug("scheduled publisher stopped")
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
			return
		case <-ticker.C:
			sp.publishDue(ctx)
		}
	}
}

func (sp *scheduledPublisher) publishDue(ctx context.Context) {
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
func (sp *scheduledPublisher) enqueueScheduled(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope, consumerGroupID string) {
	pendingKey := qKey.Pending()
	priorityKey := qKey.Priority()
	if consumerGroupID != "" {
		pendingKey = qKey.PendingWithGroup(consumerGroupID)
		priorityKey = qKey.PriorityWithGroup(consumerGroupID)
	}

	luaKeys := make([]string, 0, 7+2*len(messages))
	luaKeys = append(luaKeys,
		qKey.Properties(),
		pendingKey,
		qKey.Published(),
		priorityKey,
		qKey.Scheduled(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	)

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

	queueParsed := queue.ParsedParams{
		QueueParams: *sp.queue,
		GroupID:     consumerGroupID,
	}

	if len(deadLetteredIDs) > 0 {
		sp.log.Warn("scheduled messages dead-lettered during publication "+
			"(consumer group no longer exists)",
			"count", len(deadLetteredIDs),
			"group", consumerGroupID,
		)
		for _, deadLetteredID := range deadLetteredIDs {
			PublishMessageDeadLettered(ctx, deadLetteredID, queueParsed)
		}
	}

	for _, publishedID := range publishedIDs {
		PublishMessagePublished(ctx, publishedID, queueParsed)
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
