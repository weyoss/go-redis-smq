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
	"strconv"
	"sync"
	"time"

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	mSchema "github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// BatchAcker buffers acknowledgments and flushes them in batches.
type BatchAcker struct {
	mu             sync.Mutex
	queue          *q.QueueParams
	consumerID     string
	cfg            consumer.BatchConfig
	buffer         []string
	timer          *time.Timer
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	unacknowledger *MessageUnacknowledger
	log            *slog.Logger
}

// NewBatchAcker creates a new batch acker.
func NewBatchAcker(queue *q.QueueParams, consumerID string, cfg consumer.BatchConfig) *BatchAcker {
	return &BatchAcker{
		queue:          queue,
		consumerID:     consumerID,
		cfg:            cfg,
		buffer:         make([]string, 0, cfg.BatchSize),
		unacknowledger: NewMessageUnacknowledger(queue, consumerID),
		log:            logger.New("consumer", "batch-acker", consumerID, queue.Name()),
	}
}

// Run starts the batch acker. Must be called before Ack.
func (ba *BatchAcker) Run(ctx context.Context) {
	ba.ctx, ba.cancel = context.WithCancel(ctx)
	ba.log.Debug("batch acker started",
		"enabled", ba.cfg.Enabled,
		"batchSize", ba.cfg.BatchSize,
		"batchTimeout", ba.cfg.BatchTimeout,
	)
}

// Ack adds a message ID to the batch. Flushes if the batch is full.
func (ba *BatchAcker) Ack(messageID string) {
	if !ba.cfg.Enabled {
		ba.acknowledge([]string{messageID})
		return
	}

	ba.mu.Lock()
	defer ba.mu.Unlock()

	ba.buffer = append(ba.buffer, messageID)

	if ba.timer == nil {
		ba.timer = time.AfterFunc(ba.cfg.BatchTimeout, ba.flush)
	}

	if len(ba.buffer) >= ba.cfg.BatchSize {
		ba.log.Debug("batch full — flushing", "size", len(ba.buffer))
		if ba.timer != nil {
			ba.timer.Stop()
			ba.timer = nil
		}
		ba.flush()
	}
}

// Flush sends all buffered acknowledgments immediately.
func (ba *BatchAcker) Flush() {
	ba.mu.Lock()
	defer ba.mu.Unlock()
	ba.flush()
}

// Shutdown stops the batch acker and flushes any pending acknowledgments.
// It waits for all in‑flight acknowledgments to complete.
func (ba *BatchAcker) Shutdown() {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	ba.log.Debug("shutting down batch acker", "pending", len(ba.buffer))

	if ba.cancel != nil {
		ba.cancel()
	}

	if ba.timer != nil {
		ba.timer.Stop()
		ba.timer = nil
	}

	ba.flush()
	ba.wg.Wait()
}

func (ba *BatchAcker) flush() {
	if len(ba.buffer) == 0 {
		return
	}

	ids := make([]string, len(ba.buffer))
	copy(ids, ba.buffer)
	ba.buffer = ba.buffer[:0]

	if ba.timer != nil {
		ba.timer.Stop()
		ba.timer = nil
	}

	ba.log.Debug("flushing batch", "count", len(ids))
	ba.wg.Add(1)
	go func() {
		defer ba.wg.Done()
		ba.acknowledge(ids)
	}()
}

func (ba *BatchAcker) acknowledge(ids []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	qKey := keys.Queue{Namespace: ba.queue.NS(), Name: ba.queue.Name()}

	cfg := config.Get()

	storeMessages := "0"
	expireStoredMessages := "0"
	storedMessagesSize := "0"
	if cfg.MessageAudit.AcknowledgedMessages.Enabled {
		storeMessages = "1"
		if cfg.MessageAudit.AcknowledgedMessages.Expire > 0 {
			expireStoredMessages = strconv.Itoa(cfg.MessageAudit.AcknowledgedMessages.Expire * 1000)
		}
		if cfg.MessageAudit.AcknowledgedMessages.QueueSize > 0 {
			storedMessagesSize = fmt.Sprintf("%d", -cfg.MessageAudit.AcknowledgedMessages.QueueSize)
		}
	}

	luaKeys := []string{
		qKey.ConsumerProcessing(ba.consumerID),
		qKey.Acknowledged(),
		qKey.Properties(),
	}
	for _, id := range ids {
		luaKeys = append(luaKeys, keys.System{}.Message(id))
	}

	now := time.Now().UnixMilli()
	argv := []interface{}{
		storeMessages,
		expireStoredMessages,
		storedMessagesSize,
		now,
		qSchema.QueueFieldOperationalState.Key(),
		q.StateActive.Int(),
		q.StatePaused.Int(),
		q.StateStopped.Int(),
		q.StateLocked.Int(),
		mSchema.MessageFieldStatus.Key(),
		msg.StatusAcknowledged.Int(),
		mSchema.MessageFieldAcknowledgedAt.Key(),
		qSchema.QueueFieldAcknowledgedMessagesCount.Key(),
		qSchema.QueueFieldProcessingMessagesCount.Key(),
	}
	for _, id := range ids {
		argv = append(argv, id)
	}

	reply, err := redisClient.Eval(ctx, scripts.AcknowledgeMessage, luaKeys, argv...)
	if err != nil {
		ba.log.Error("batch ack script failed", "count", len(ids), "error", err)
		ba.unacknowledgeFailed(ctx, ids, CauseUnexpectedError)
		return
	}

	// The script returns either a string error or an array of integers (1=success, 0=not found in processing).
	if errStr, ok := reply.(string); ok {
		cause := ba.causeFromAckError(errStr)
		ba.log.Warn("batch ack rejected", "reply", errStr, "cause", int(cause))
		ba.unacknowledgeFailed(ctx, ids, cause)
		return
	}

	results, ok := reply.([]interface{})
	if !ok {
		ba.log.Error("unexpected ack script reply", "reply", fmt.Sprintf("%v", reply))
		ba.unacknowledgeFailed(ctx, ids, CauseUnexpectedError)
		return
	}

	var succeeded, alreadyHandled int
	for i, res := range results {
		if i >= len(ids) {
			break
		}
		val, _ := strconv.Atoi(fmt.Sprintf("%v", res))
		if val == 1 {
			succeeded++
		} else {
			alreadyHandled++
		}
	}

	if alreadyHandled > 0 {
		ba.log.Debug("batch ack: some messages were already handled",
			"total", len(ids), "succeeded", succeeded, "alreadyHandled", alreadyHandled)
	}
	ba.log.Debug("batch ack completed", "total", len(ids), "succeeded", succeeded)
}

func (ba *BatchAcker) unacknowledgeFailed(ctx context.Context, ids []string, cause UnacknowledgeCause) {
	var entries []UnackEntry
	for _, id := range ids {
		msgKey := keys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "message")
		if err != nil {
			ba.log.Debug("failed to load message for unacknowledgment", "messageID", id, "error", err)
			continue
		}
		codec := internalMessage.NewEnvelopeCodec()
		env, err := codec.DecodeHash(ctx, hash)
		if err != nil {
			ba.log.Debug("failed to decode message for unacknowledgment", "messageID", id, "error", err)
			continue
		}
		entries = append(entries, UnackEntry{Message: env, Cause: cause})
	}

	if len(entries) > 0 {
		if err := ba.unacknowledger.UnacknowledgeBatch(ctx, entries); err != nil {
			ba.log.Error("failed to unacknowledge after ack failure", "count", len(entries), "error", err)
		}
	}
}

func (ba *BatchAcker) causeFromAckError(reply string) UnacknowledgeCause {
	switch reply {
	case "QUEUE_STOPPED":
		return CauseQueueStopped
	case "QUEUE_LOCKED":
		return CauseQueueLocked
	case "QUEUE_INVALID_STATE":
		return CauseQueueInvalidState
	default:
		return CauseUnexpectedError
	}
}
