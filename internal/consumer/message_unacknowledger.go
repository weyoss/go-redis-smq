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
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/config"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// UnackEntry holds a message and the cause for its unacknowledgment.
type UnackEntry struct {
	Message *internalMessage.Envelope
	Cause   UnacknowledgeCause
}

// MessageUnacknowledger handles unacknowledging messages for a consumer.
type MessageUnacknowledger struct {
	queue      *queue.Params
	consumerID string
	log        *slog.Logger
}

// NewMessageUnacknowledger creates a new unacknowledger.
func NewMessageUnacknowledger(queue *queue.Params, consumerID string) *MessageUnacknowledger {
	return &MessageUnacknowledger{
		queue:      queue,
		consumerID: consumerID,
		log:        logger.New("consumer", "unacknowledger", consumerID, queue.Name()),
	}
}

// UnacknowledgeProcessingQueue unacknowledges all messages in a consumer's
// processing queue with the given cause.
func (mu *MessageUnacknowledger) UnacknowledgeProcessingQueue(ctx context.Context, cause UnacknowledgeCause) error {
	qKey := redisKeys.Queue{
		Namespace: mu.queue.NS(),
		Name:      mu.queue.Name(),
	}
	processingKey := qKey.ConsumerProcessing(mu.consumerID)

	ids, err := redisClient.Client().LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		mu.log.Error("failed to fetch processing queue", "error", err)
		return nil
	}
	if len(ids) == 0 {
		return nil
	}

	mu.log.Debug("unacknowledging processing queue", "count", len(ids), "cause", int(cause))

	messages, err := mu.loadMessages(ctx, ids)
	if err != nil {
		mu.log.Error("failed to load messages", "error", err)
		return fmt.Errorf("load messages: %w", err)
	}

	entries := make([]UnackEntry, len(messages))
	for i, msg := range messages {
		entries[i] = UnackEntry{Message: msg, Cause: cause}
	}

	return mu.UnacknowledgeBatch(ctx, entries)
}

// UnacknowledgeBatch unacknowledges multiple messages in a single Lua script call.
func (mu *MessageUnacknowledger) UnacknowledgeBatch(ctx context.Context, entries []UnackEntry) error {
	if len(entries) == 0 {
		return nil
	}

	mu.log.Debug("unacknowledging batch", "count", len(entries))

	qKey := redisKeys.Queue{
		Namespace: mu.queue.NS(),
		Name:      mu.queue.Name(),
	}

	luaKeys, luaArgs := mu.buildBatchLuaArgs(ctx, qKey, entries)

	_, err := redisClient.Eval(ctx, scripts.UnacknowledgeMessage, luaKeys, luaArgs...)
	if err != nil {
		mu.log.Error("unacknowledge script failed", "count", len(entries), "error", err)
		return err
	}

	mu.log.Debug("batch unacknowledged", "count", len(entries))

	return nil
}

// buildBatchLuaArgs builds the KEYS and ARGV for the UNACKNOWLEDGE_MESSAGE Lua script.
func (mu *MessageUnacknowledger) buildBatchLuaArgs(
	_ context.Context,
	qKey redisKeys.Queue,
	entries []UnackEntry,
) ([]string, []interface{}) {
	cfg := config.Get()
	now := time.Now().UnixMilli()

	storeMessages := "0"
	expireStoredMessages := "0"
	storedMessagesSize := "0"
	if cfg.MessageAudit.DeadLetteredMessages.Enabled {
		storeMessages = "1"
		if cfg.MessageAudit.DeadLetteredMessages.Expire > 0 {
			expireStoredMessages = strconv.Itoa(cfg.MessageAudit.DeadLetteredMessages.Expire * 1000)
		}
		if cfg.MessageAudit.DeadLetteredMessages.QueueSize > 0 {
			storedMessagesSize = fmt.Sprintf("%d", -cfg.MessageAudit.DeadLetteredMessages.QueueSize)
		}
	}

	maxHistorySize := 0
	if cfg.MessageAudit.UnacknowledgementHistory.Enabled {
		maxHistorySize = cfg.MessageAudit.UnacknowledgementHistory.MaxSize
	}

	luaKeys := []string{
		qKey.Requeued(),
		qKey.DeadLetter(),
		qKey.Properties(),
	}

	staticArgv := []interface{}{
		strconv.Itoa(int(ActionDelay)),                         // ARGV[1]: ERetryActionDelay
		strconv.Itoa(int(ActionRequeue)),                       // ARGV[2]: ERetryActionRequeue
		storeMessages,                                          // ARGV[3]
		expireStoredMessages,                                   // ARGV[4]
		storedMessagesSize,                                     // ARGV[5]
		internalMessage.MessageFieldStatus.Key(),               // ARGV[6]
		qSchema.QueueFieldProcessingMessagesCount.Key(),        // ARGV[7]
		qSchema.QueueFieldDeadLetteredMessagesCount.Key(),      // ARGV[8]
		qSchema.QueueFieldRequeuedMessagesCount.Key(),          // ARGV[9]
		publicmessage.StatusUnackRequeuing.Int(),               // ARGV[10]
		publicmessage.StatusDeadLettered.Int(),                 // ARGV[11]
		internalMessage.MessageFieldDeadLetteredAt.Key(),       // ARGV[12]
		internalMessage.MessageFieldUnacknowledgedAt.Key(),     // ARGV[13]
		internalMessage.MessageFieldLastUnacknowledgedAt.Key(), // ARGV[14]
		internalMessage.MessageFieldExpired.Key(),              // ARGV[15]
		qSchema.QueueFieldOperationalState.Key(),               // ARGV[16]
		queue.StateActive.Int(),                                // ARGV[17]
		queue.StatePaused.Int(),                                // ARGV[18]
		queue.StateStopped.Int(),                               // ARGV[19]
		queue.StateLocked.Int(),                                // ARGV[20]
		strconv.Itoa(maxHistorySize),                           // ARGV[21]
	}

	deadLetteredCount := 0
	requeuedCount := 0
	delayedCount := 0

	for _, entry := range entries {
		msgID := entry.Message.ID()
		msgKey := redisKeys.System{}.Message(msgID)
		historyKey := redisKeys.System{}.MessageAcknowledgementHistory(msgID)
		state := entry.Message.MessageState()

		action, _ := resolveUnackAction(entry.Message, entry.Cause)

		retryAction := ""
		if action != ActionDeadLetter {
			retryAction = strconv.Itoa(int(action))
		}

		deadLetteredAt := ""
		switch action {
		case ActionDeadLetter:
			deadLetteredAt = fmt.Sprintf("%d", now)
			deadLetteredCount++
		case ActionDelay:
			delayedCount++
		default:
			requeuedCount++
		}

		messageExpired := "0"
		if entry.Message.IsExpired() || entry.Cause == CauseTTLExpired {
			messageExpired = "1"
		}

		historyRecord := ""
		if cfg.MessageAudit.UnacknowledgementHistory.Enabled {
			record, _ := json.Marshal(map[string]interface{}{
				"messageId": msgID,
				"cause":     int(entry.Cause),
				"action":    int(action),
				"attempts":  state.Attempts(),
				"timestamp": now,
			})
			historyRecord = string(record)
		}

		luaKeys = append(luaKeys, qKey.ConsumerProcessing(mu.consumerID), msgKey, historyKey)

		staticArgv = append(staticArgv,
			msgID,
			retryAction,
			deadLetteredAt,
			messageExpired,
			fmt.Sprintf("%d", now),
			fmt.Sprintf("%d", now),
			historyRecord,
		)
	}

	mu.log.Debug("built unacknowledge batch",
		"total", len(entries),
		"deadLettered", deadLetteredCount,
		"delayed", delayedCount,
		"requeued", requeuedCount,
	)

	return luaKeys, staticArgv
}

// loadMessages loads full message envelopes from Redis by their IDs.
func (mu *MessageUnacknowledger) loadMessages(ctx context.Context, ids []string) ([]*internalMessage.Envelope, error) {
	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := redisKeys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "message")
		if err != nil {
			mu.log.Debug("message not found during load", "messageID", id)
			continue
		}
		codec := internalMessage.NewEnvelopeCodec()
		env, err := codec.DecodeHash(ctx, hash)
		if err != nil {
			mu.log.Debug("failed to decode message during load", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}
	return messages, nil
}
