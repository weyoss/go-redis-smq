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

	"github.com/redis/go-redis/v9"
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

// delayedRequeuer periodically moves due messages from the delayed
// sorted set back to the pending queue.
//
// Queue-scoped worker: spawned by Coordinator for every queue whose
// WorkersLock this process holds. Runs until ctx is cancelled (by lease
// loss, queue removal, or coordinator shutdown).
//
// Idempotence: the per-message ZREM gate in requeue-delayed.lua
// (`if redis.call("ZREM", keyQueueDelayed, messageId) == 1`) ensures
// only one executor moves each message. Safe under concurrent execution
// during the ~10s split-brain window that follows a lease loss.
type delayedRequeuer struct {
	queue    *queue.Params
	interval time.Duration
	log      *slog.Logger
}

// newDelayedRequeuer constructs the worker for the given queue.
//
// No consumer ID: the requeuer is not tied to a consumer instance. It
// moves messages that were previously unacknowledged (by any consumer
// with a retry delay) back into the pending queue when their delay
// expires.
func newDelayedRequeuer(qp *queue.Params, interval time.Duration) *delayedRequeuer {
	return &delayedRequeuer{
		queue:    qp,
		interval: interval,
		log:      logger.New("worker", "delayed-requeuer", qp.Name()),
	}
}

// Run blocks until ctx is cancelled. Spawned as a goroutine by
// queueWorkers.trySpawnWorkers; the coordinator's per-queue wg tracks
// this Run call, so wg.Wait() waits for the ticker loop (including any
// in-flight requeue) to exit.
//
// Recover is local: a panic in requeue kills this worker's goroutine
// but does not crash the process. The coordinator's reconcile tick will
// re-elect on the next cycle if the lease is still held (it isn't —
// the panic handler returns from Run, wg.Done fires, but the lease is
// still held by Coordinator.active until the next reconcile sees the
// goroutine is gone. This is a known limitation of the current design
// for any worker panic; addressing it would require the coordinator to
// detect worker death, which it doesn't today).
func (dr *delayedRequeuer) Run(ctx context.Context) {
	dr.log.Debug("starting delayed requeuer", "interval", dr.interval)
	defer dr.log.Debug("delayed requeuer stopped")
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
			return
		case <-ticker.C:
			dr.requeue(ctx)
		}
	}
}

func (dr *delayedRequeuer) requeue(ctx context.Context) {
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

	dr.log.Debug("found due delayed messages", "count", len(ids))

	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := keys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "delayed message")
		if err != nil {
			dr.log.Debug("failed to load delayed message", "messageID", id, "error", err)
			continue
		}
		codec := internalMessage.NewCodec()
		env, err := codec.DecodeEnvelope(ctx, hash)
		if err != nil {
			dr.log.Debug("failed to decode delayed message", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}

	if len(messages) == 0 {
		return
	}

	dr.requeueMessages(ctx, qKey, messages)
}

func (dr *delayedRequeuer) requeueMessages(ctx context.Context, qKey keys.Queue, messages []*internalMessage.Envelope) {
	luaKeys := make([]string, 0, 4+3*len(messages))
	luaKeys = append(luaKeys,
		qKey.Properties(),
		qKey.Delayed(),
		qKey.DeadLetter(),
		qKey.ConsumerGroups(),
	)

	timestamp := time.Now().UnixMilli()

	argv := make([]interface{}, 0, 17+3*len(messages))
	argv = append(argv,
		qSchema.Type.Key(),
		qSchema.DelayedMessagesCount.Key(),
		qSchema.PendingMessagesCount.Key(),
		qSchema.DeadLetteredMessagesCount.Key(),
		schema.Status.Key(),
		publicmessage.StatusPending.Int(),
		publicmessage.StatusDeadLettered.Int(),
		schema.DeadLetteredAt.Key(),
		schema.LastRetriedAttemptAt.Key(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.OperationalState.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		timestamp,
	)

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
			consumerGroupID,
		)
		requeuedCount++
	}

	dr.log.Debug("requeuing delayed messages",
		"total", len(messages),
		"may_dead_letter", deadLetteredCount, // placeholder; actual count comes from Lua
	)

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
}
