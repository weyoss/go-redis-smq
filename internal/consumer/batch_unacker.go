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
	"log/slog"
	"sync"
	"time"

	consumerEvents "github.com/weyoss/go-redis-smq/internal/consumer/events"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// unackEntry is a pending unacknowledgment entry.
type unackEntry struct {
	msg   *internalMessage.Envelope
	cause consumer.UnacknowledgeCause
}

// BatchUnacker buffers unacknowledgments and flushes them in batches.
//
// It improves throughput by grouping multiple unacknowledgment operations
// into a single Redis Lua script call when batch mode is enabled.
type BatchUnacker struct {
	mu             sync.Mutex
	queue          *queue.Params
	groupID        string
	consumerID     string
	cfg            consumer.BatchConfig
	buffer         []unackEntry
	timer          *time.Timer
	ctx            context.Context
	cancel         context.CancelFunc
	unacknowledger *MessageUnacknowledger
	wg             sync.WaitGroup // tracks in-flight unacknowledge goroutines
	log            *slog.Logger
}

// NewBatchUnacker creates a new batch unacknowledger.
func NewBatchUnacker(queue *queue.Params, groupID, consumerID string, cfg consumer.BatchConfig) *BatchUnacker {
	return &BatchUnacker{
		queue:          queue,
		groupID:        groupID,
		consumerID:     consumerID,
		cfg:            cfg,
		buffer:         make([]unackEntry, 0, cfg.BatchSize),
		unacknowledger: NewMessageUnacknowledger(queue, consumerID),
		log:            logger.New("consumer", "batch-unacker", consumerID, queue.Name()),
	}
}

// Run starts the batch unacker.
func (bu *BatchUnacker) Run(ctx context.Context) {
	bu.ctx, bu.cancel = context.WithCancel(ctx)
	bu.log.Debug("batch unacker started",
		"enabled", bu.cfg.Enabled,
		"batchSize", bu.cfg.BatchSize,
		"batchTimeout", bu.cfg.BatchTimeout,
	)
}

// Unack adds a message to the unacknowledgment batch or processes it
// immediately if batching is disabled.
func (bu *BatchUnacker) Unack(msg *internalMessage.Envelope, cause consumer.UnacknowledgeCause) {
	if !bu.cfg.Enabled {
		bu.unacknowledge([]unackEntry{{msg: msg, cause: cause}})
		return
	}

	bu.mu.Lock()
	defer bu.mu.Unlock()

	bu.buffer = append(bu.buffer, unackEntry{msg: msg, cause: cause})

	if bu.timer == nil {
		bu.timer = time.AfterFunc(bu.cfg.BatchTimeout, bu.flushFromTimer)
	}

	if len(bu.buffer) >= bu.cfg.BatchSize {
		bu.log.Debug("batch full — flushing", "size", len(bu.buffer))
		if bu.timer != nil {
			bu.timer.Stop()
			bu.timer = nil
		}
		bu.flush()
	}
}

// Flush sends all buffered unacknowledgments immediately.
func (bu *BatchUnacker) Flush() {
	bu.mu.Lock()
	defer bu.mu.Unlock()
	bu.flush()
}

// Shutdown stops the batch unacker and flushes any pending
// unacknowledgments.
func (bu *BatchUnacker) Shutdown() {
	bu.mu.Lock()
	defer bu.mu.Unlock()

	bu.log.Debug("shutting down batch unacker", "pending", len(bu.buffer))

	if bu.cancel != nil {
		bu.cancel()
	}

	if bu.timer != nil {
		bu.timer.Stop()
		bu.timer = nil
	}

	bu.flush()
	bu.wg.Wait()
}

// flushFromTimer is the time.AfterFunc callback. It must acquire bu.mu
// because flush assumes the caller holds it — every other caller (Unack,
// Shutdown, Flush) does.
func (bu *BatchUnacker) flushFromTimer() {
	bu.mu.Lock()
	defer bu.mu.Unlock()
	bu.flush()
}

// flush sends the current buffer to the unacknowledger in a separate
// goroutine and resets the buffer.
func (bu *BatchUnacker) flush() {
	if len(bu.buffer) == 0 {
		return
	}

	entries := make([]unackEntry, len(bu.buffer))
	copy(entries, bu.buffer)
	bu.buffer = bu.buffer[:0]

	if bu.timer != nil {
		bu.timer.Stop()
		bu.timer = nil
	}

	bu.log.Debug("flushing batch", "count", len(entries))
	bu.wg.Add(1)
	go func() {
		defer bu.wg.Done()
		bu.unacknowledge(entries)
	}()
}

// unacknowledge processes a batch of unacknowledgments and publishes the
// corresponding consumer events.
//
// Uses the UnackResult returned by UnacknowledgeBatch directly — the
// Action is what the Lua actually executed, the Cause is what
// buildBatchLuaArgs computed from the same sample of the message envelope
// that produced the ARGV the Lua acted on. Neither is recomputed here.
//
// Pre-fix, this function called resolveUnackAction a second time after the
// Lua had already acted. The second call sampled IsExpired() at a later
// wall-clock instant than the first, so a message whose TTL expired in the
// interim would be requeued by the Lua but reported as dead-lettered by
// this function — a divergence between the event stream and the state in
// Redis. The same recomputation also doubled the CPU cost of
// resolveUnackAction per unack (once in buildBatchLuaArgs, once here) and
// meant the two sites could drift if the function were ever changed.
//
// Post-fix, resolveUnackAction is called exactly once per entry, in
// buildBatchLuaArgs. The UnackResult carries both the Lua-reported action
// and the Go-computed cause, so neither is derived from the envelope after
// the Eval returns.
//
// Event publishing is also filtered by UnackResult.Processed: entries the
// Lua skipped (message not in the consumer's processing queue — already
// recovered by a peer's ReapConsumers cycle or a concurrent batch) get no
// event. Pre-fix, events fired for every submitted entry regardless, so
// monitoring and audit subscribers saw phantom events for messages that
// were never transitioned by this batch. That was H7.
func (bu *BatchUnacker) unacknowledge(entries []unackEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unackEntries := make([]UnackEntry, len(entries))
	for i, entry := range entries {
		unackEntries[i] = UnackEntry{
			Message: entry.msg,
			Cause:   entry.cause,
		}
	}

	results, err := bu.unacknowledger.UnacknowledgeBatch(ctx, unackEntries)
	if err != nil {
		bu.log.Error("batch unack failed", "count", len(entries), "error", err)
		return
	}

	processed := 0
	for _, r := range results {
		if r.Processed {
			processed++
		}
	}
	bu.log.Debug("batch unack completed",
		"count", len(entries),
		"processed", processed,
	)

	// Build the queue.ParsedParams once for all event publishes below. The
	// queue is nested under "queueParams" with the effective consumer group
	// ID alongside it, so cross-language subscribers see a consistent
	// payload shape.
	queueParsed := queue.ParsedParams{
		QueueParams: *bu.queue,
		GroupID:     bu.groupID,
	}

	for i, entry := range entries {
		// Defensive: if the Lua returned fewer results than entries (should
		// not happen given the KEYS/ARGV validation, but be safe), treat
		// out-of-range indices as skipped.
		if i >= len(results) || !results[i].Processed {
			// Lua skipped this entry: the message was not in the consumer's
			// processing queue at flush time — already handled elsewhere.
			// Do NOT publish events for it; there was no state transition
			// to report.
			bu.log.Debug("skipping phantom unack event",
				"messageID", entry.msg.ID(),
				"cause", int(entry.cause),
			)
			continue
		}

		msgID := entry.msg.ID()
		action := results[i].Action
		deadLetterCause := results[i].Cause

		switch action {
		case ActionDeadLetter:
			bu.log.Warn("message dead-lettered",
				"messageID", msgID,
				"cause", int(entry.cause),
				"deadLetterCause", int(deadLetterCause),
			)
			consumerEvents.PublishMessageUnacknowledged(ctx, msgID, queueParsed, bu.consumerID, int(entry.cause))
			consumerEvents.PublishMessageDeadLettered(ctx, msgID, queueParsed, bu.consumerID, int(deadLetterCause))

		case ActionDelay:
			bu.log.Debug("message delayed for retry",
				"messageID", msgID,
				"cause", int(entry.cause),
			)
			consumerEvents.PublishMessageUnacknowledged(ctx, msgID, queueParsed, bu.consumerID, int(entry.cause))
			consumerEvents.PublishMessageDelayed(ctx, msgID, queueParsed, bu.consumerID)

		case ActionRequeue:
			bu.log.Debug("message requeued",
				"messageID", msgID,
				"cause", int(entry.cause),
			)
			consumerEvents.PublishMessageUnacknowledged(ctx, msgID, queueParsed, bu.consumerID, int(entry.cause))
			consumerEvents.PublishMessageRequeued(ctx, msgID, queueParsed, bu.consumerID)

		default:
			// Should not happen: the Lua's action field is one of
			// ACTION_DEAD_LETTER (0), ACTION_REQUEUE (1), ACTION_DELAY (2).
			// Log and skip rather than panic — a future Lua change adding a
			// new action value should not crash the consumer.
			bu.log.Error("unexpected unack action from Lua",
				"messageID", msgID,
				"action", int(action),
			)
		}
	}
}
