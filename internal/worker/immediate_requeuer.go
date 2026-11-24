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
	"fmt"
	"log/slog"
	"time"

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// immediateRequeuer periodically moves unacknowledged messages from the
// requeued list back to the pending queue (immediate retry) or to the
// delayed sorted set (retry with backoff).
//
// Queue-scoped worker: spawned by Coordinator for every queue whose
// WorkersLock this process holds. Runs until ctx is cancelled (by lease
// loss, queue removal, or coordinator shutdown).
//
// Idempotence: requeue-immediate.lua gates each message with
//
//	if redis.call("LREM", keyQueueRequeued, 1, messageId) == 1 then
//
// Only one executor per message proceeds; all others skip. Safe under
// concurrent execution during the ~10s split-brain window after a lease
// loss.
type immediateRequeuer struct {
	queue    *queue.Params
	interval time.Duration
	log      *slog.Logger
}

// newImmediateRequeuer constructs the worker for the given queue.
//
// No consumer ID: the requeuer is not tied to a consumer instance. The
// messages it moves were previously unacknowledged by any consumer —
// it doesn't matter which one.
func newImmediateRequeuer(qp *queue.Params, interval time.Duration) *immediateRequeuer {
	return &immediateRequeuer{
		queue:    qp,
		interval: interval,
		log:      logger.New("worker", "immediate-requeuer", qp.Name()),
	}
}

// Run blocks until ctx is cancelled. Spawned as a goroutine by
// queueWorkers.trySpawnWorkers; the coordinator's per-queue wg tracks
// this Run call, so wg.Wait() waits for the ticker loop (including any
// in-flight requeue) to exit.
//
// Recover is local: a panic in requeue kills this worker's goroutine
// but does not crash the process. The coordinator's reconcile tick will
// re-elect on the next cycle if the lease is still held — with the same
// caveat noted in scheduled_publisher.go about worker panics leaving the
// lease held until the next reconcile.
func (ir *immediateRequeuer) Run(ctx context.Context) {
	ir.log.Debug("starting immediate requeuer", "interval", ir.interval)
	defer ir.log.Debug("immediate requeuer stopped")
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
			return
		case <-ticker.C:
			ir.requeue(ctx)
		}
	}
}

func (ir *immediateRequeuer) requeue(ctx context.Context) {
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
		codec := internalMessage.NewCodec()
		env, err := codec.DecodeEnvelope(ctx, hash)
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

func (ir *immediateRequeuer) requeueMessages(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope) {
	luaKeys := []string{
		qKey.Properties(),
		qKey.Requeued(),
		qKey.Delayed(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	}

	timestamp := time.Now().UnixMilli()

	argv := []interface{}{
		qSchema.Type.Key(),
		qSchema.RequeuedMessagesCount.Key(),
		qSchema.DelayedMessagesCount.Key(),
		qSchema.PendingMessagesCount.Key(),
		qSchema.DeadLetteredMessagesCount.Key(),
		schema.Status.Key(),
		publicmessage.StatusPending.Int(),
		publicmessage.StatusDeadLettered.Int(),
		schema.DeadLetteredAt.Key(),
		publicmessage.StatusUnackDelaying.Int(),
		schema.LastRetriedAttemptAt.Key(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.OperationalState.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		timestamp,
	}

	pendingCount := 0
	delayedCount := 0

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
}
