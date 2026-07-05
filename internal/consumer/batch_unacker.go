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
	"github.com/weyoss/go-redis-smq/pkg/consumer/c"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type unackEntry struct {
	msg   *internalMessage.Envelope
	cause UnacknowledgeCause
}

type BatchUnacker struct {
	mu             sync.Mutex
	queue          *q.QueueParams
	groupID        string
	consumerID     string
	cfg            c.BatchConfig
	buffer         []unackEntry
	timer          *time.Timer
	ctx            context.Context
	cancel         context.CancelFunc
	unacknowledger *MessageUnacknowledger
	wg             sync.WaitGroup // tracks in-flight unacknowledge goroutines
	log            *slog.Logger
}

func NewBatchUnacker(queue *q.QueueParams, groupID, consumerID string, cfg c.BatchConfig) *BatchUnacker {
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

func (bu *BatchUnacker) Run(ctx context.Context) {
	bu.ctx, bu.cancel = context.WithCancel(ctx)
	bu.log.Debug("batch unacker started",
		"enabled", bu.cfg.Enabled,
		"batchSize", bu.cfg.BatchSize,
		"batchTimeout", bu.cfg.BatchTimeout,
	)
}

func (bu *BatchUnacker) Unack(msg *internalMessage.Envelope, cause UnacknowledgeCause) {
	if !bu.cfg.Enabled {
		bu.unacknowledge([]unackEntry{{msg: msg, cause: cause}})
		return
	}

	bu.mu.Lock()
	defer bu.mu.Unlock()

	bu.buffer = append(bu.buffer, unackEntry{msg: msg, cause: cause})

	if bu.timer == nil {
		bu.timer = time.AfterFunc(bu.cfg.BatchTimeout, bu.flush)
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

func (bu *BatchUnacker) Flush() {
	bu.mu.Lock()
	defer bu.mu.Unlock()
	bu.flush()
}

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

	if err := bu.unacknowledger.UnacknowledgeBatch(ctx, unackEntries); err != nil {
		bu.log.Error("batch unack failed", "count", len(entries), "error", err)
		return
	}

	bu.log.Debug("batch unack completed", "count", len(entries))

	basePayload := consumerEvents.MessagePayload{
		Queue:            *bu.queue,
		GroupID:          bu.groupID,
		MessageHandlerID: bu.consumerID,
		ConsumerID:       bu.consumerID,
	}

	for _, entry := range entries {
		msgID := entry.msg.ID()
		action, deadLetterCause := resolveUnackAction(entry.msg, entry.cause)

		basePayload.MessageID = msgID

		consumerEvents.PublishMessageUnacknowledged(ctx, consumerEvents.MessageUnacknowledgedPayload{
			MessagePayload: basePayload,
			Cause:          int(entry.cause),
		})

		switch action {
		case ActionDeadLetter:
			bu.log.Warn("message dead-lettered",
				"messageID", msgID,
				"cause", int(entry.cause),
				"deadLetterCause", int(deadLetterCause),
			)
			consumerEvents.PublishMessageDeadLettered(ctx, consumerEvents.MessageDeadLetteredPayload{
				MessagePayload: basePayload,
				Cause:          int(deadLetterCause),
			})
		case ActionDelay:
			bu.log.Debug("message delayed for retry",
				"messageID", msgID,
				"cause", int(entry.cause),
			)
			consumerEvents.PublishMessageDelayed(ctx, basePayload)
		case ActionRequeue:
			bu.log.Debug("message requeued",
				"messageID", msgID,
				"cause", int(entry.cause),
			)
			consumerEvents.PublishMessageRequeued(ctx, basePayload)
		}
	}
}
