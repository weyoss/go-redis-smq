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
	"github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	redisKeys "github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// UnackEntry holds a message and the cause for its unacknowledgment.
type UnackEntry struct {
	Message *internalMessage.Envelope
	Cause   consumer.UnacknowledgeCause
}

// UnackResult is the per-message outcome of an UnacknowledgeBatch call.
//
// Fields:
//
//   - Processed: true if the Lua removed the message from ConsumerProcessing
//     and applied the action; false if it skipped it (already handled
//     elsewhere — a peer's ReapConsumers cycle, a concurrent batch, etc.).
//     Callers MUST check this before publishing any unack-related event.
//     Populated from the Lua reply, which is the only authoritative source
//     for "did this script actually transition this message."
//
//   - Action: the action the script was asked to execute (ActionDeadLetter,
//     ActionRequeue, or ActionDelay). Populated from the single
//     resolveUnackAction call made in buildBatchLuaArgs — the same call
//     that produced the retryAction ARGV the Lua acted on. Not recomputed
//     after the Eval returns, and not echoed back from the Lua (the Lua
//     treats Requeue and Delay identically — both RPUSH to the requeue
//     list — so it has no independent notion of "which action" to report).
//     Meaningful only when Processed is true.
//
//   - Cause: the dead-letter cause for ActionDeadLetter entries
//     (DeadLetterTTLExpired / DeadLetterRetryThresholdExceeded /
//     DeadLetterPeriodicMessage). Populated from the same buildBatchLuaArgs
//     resolveUnackAction call. Meaningful only when Processed is true and
//     Action is ActionDeadLetter.
//
// The TTL-boundary divergence that pre-fix existed between ARGV construction
// and event publishing (IsExpired() sampled twice at different instants)
// is closed by design: resolveUnackAction is called exactly once per entry,
// here in buildBatchLuaArgs, and its outputs flow through UnackResult
// unchanged.
type UnackResult struct {
	Processed bool
	Action    UnacknowledgeAction
	Cause     consumer.DeadLetterCause
}

// unackPrecompute is the internal per-entry payload threaded from
// buildBatchLuaArgs to UnackResult population. It carries the same Action
// and Cause that drove the ARGV, so UnackResult can be populated without a
// second resolveUnackAction call.
type unackPrecompute struct {
	Action UnacknowledgeAction
	Cause  consumer.DeadLetterCause
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
//
// The UnackResult slice is discarded — this method is used by
// ReapConsumers to recover a dead consumer's in-flight messages, where
// event publishing is handled elsewhere (or not at all). Any entry the
// Lua skips (already recovered by another peer) is benign; the caller
// does not need per-message outcomes.
func (mu *MessageUnacknowledger) UnacknowledgeProcessingQueue(ctx context.Context, cause consumer.UnacknowledgeCause) error {
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

	_, err = mu.UnacknowledgeBatch(ctx, entries)
	return err
}

// UnacknowledgeBatch unacknowledges multiple messages in a single Lua
// script call.
//
// Returns one UnackResult per entry, in the same order as entries. On any
// script-level error (Redis failure, string error reply like QUEUE_LOCKED,
// or malformed reply), returns (nil, err).
//
// Reply shape (matches internal/redis/scripts/scripts/core/unacknowledge-message.lua):
//
//	[]interface{} of int64, one element per submitted entry:
//	  1 = the Lua removed the message from ConsumerProcessing and applied
//	      the action
//	  0 = the Lua skipped the message (already handled elsewhere)
//
//	len(reply) == len(entries) is required; any other length indicates a
//	Lua/Go contract drift and is returned as an error. Do NOT change the
//	Lua to return per-entry pairs without also updating this parser — the
//	length check will reject the batch and silently drop the unack path.
//
// The Processed field comes from the Lua reply — it's the only
// authoritative source for "did this script actually transition this
// message." Action and Cause come from the single resolveUnackAction
// call made in buildBatchLuaArgs — the same call that produced the
// retryAction ARGV the Lua acted on. They are NOT recomputed after the
// Eval returns.
//
// Callers MUST use these values directly. Do not re-derive the action or
// cause from the message envelope; the envelope's IsExpired() is
// time-dependent and would produce a different answer at this point in
// the flow.
func (mu *MessageUnacknowledger) UnacknowledgeBatch(ctx context.Context, entries []UnackEntry) ([]UnackResult, error) {
	if len(entries) == 0 {
		return []UnackResult{}, nil
	}

	mu.log.Debug("unacknowledging batch", "count", len(entries))

	qKey := redisKeys.Queue{
		Namespace: mu.queue.NS(),
		Name:      mu.queue.Name(),
	}

	luaKeys, luaArgs, precompute := mu.buildBatchLuaArgs(ctx, qKey, entries)

	reply, err := redisClient.Eval(ctx, scripts.UnacknowledgeMessage, luaKeys, luaArgs...)
	if err != nil {
		mu.log.Error("unacknowledge script failed", "count", len(entries), "error", err)
		return nil, err
	}

	// String reply = script-level error (QUEUE_NOT_FOUND, QUEUE_LOCKED,
	// QUEUE_STOPPED, QUEUE_INVALID_STATE, INVALID_ARGS_ERROR). Pre-fix,
	// these were silently discarded and treated as success; the parser
	// rewrite that implements H7 also fixes that class of bug.
	if errStr, ok := reply.(string); ok {
		mu.log.Warn("unacknowledge script rejected", "count", len(entries), "reply", errStr)
		return nil, fmt.Errorf("unacknowledge: %s", errStr)
	}

	arr, ok := reply.([]interface{})
	if !ok {
		mu.log.Error("unexpected unacknowledge reply", "reply", fmt.Sprintf("%v", reply))
		return nil, fmt.Errorf("unacknowledge: unexpected reply type %T", reply)
	}

	// Result-only reply: one element per entry (1 = processed, 0 = skipped).
	if len(arr) != len(entries) {
		mu.log.Error("unacknowledge reply element count mismatch",
			"entries", len(entries), "reply_len", len(arr))
		return nil, fmt.Errorf("unacknowledge: reply has %d elements, expected %d for %d entries",
			len(arr), len(entries), len(entries))
	}

	results := make([]UnackResult, len(entries))
	for i := 0; i < len(entries); i++ {
		resultVal, ok := arr[i].(int64)
		if !ok {
			mu.log.Error("unexpected unacknowledge element type",
				"index", i, "type", fmt.Sprintf("%T", arr[i]))
			return nil, fmt.Errorf("unacknowledge: unexpected element type at index %d", i)
		}
		results[i] = UnackResult{
			Processed: resultVal == 1,
			Action:    precompute[i].Action,
			Cause:     precompute[i].Cause,
		}
	}

	processed := 0
	for _, r := range results {
		if r.Processed {
			processed++
		}
	}
	mu.log.Debug("batch unacknowledged",
		"count", len(entries),
		"processed", processed,
	)

	return results, nil
}

// buildBatchLuaArgs builds KEYS, ARGV, and a parallel precompute slice for
// the UNACKNOWLEDGE_MESSAGE script.
//
// resolveUnackAction is called exactly once per entry here. Its action
// output drives the retryAction and deadLetteredAt ARGV values that the
// Lua acts on; both its action and cause outputs are returned as
// per-entry precompute values, so UnackResult can carry them without
// recomputation.
func (mu *MessageUnacknowledger) buildBatchLuaArgs(
	_ context.Context,
	qKey redisKeys.Queue,
	entries []UnackEntry,
) ([]string, []interface{}, []unackPrecompute) {
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
		strconv.Itoa(int(ActionDelay)),
		strconv.Itoa(int(ActionRequeue)),
		storeMessages,
		expireStoredMessages,
		storedMessagesSize,
		schema.Status.Key(),
		qSchema.ProcessingMessagesCount.Key(),
		qSchema.DeadLetteredMessagesCount.Key(),
		qSchema.RequeuedMessagesCount.Key(),
		publicmessage.StatusUnackRequeuing.Int(),
		publicmessage.StatusDeadLettered.Int(),
		schema.DeadLetteredAt.Key(),
		schema.UnacknowledgedAt.Key(),
		schema.LastUnacknowledgedAt.Key(),
		schema.Expired.Key(),
		qSchema.OperationalState.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
		strconv.Itoa(maxHistorySize),
	}

	precompute := make([]unackPrecompute, len(entries))

	for i, entry := range entries {
		msgID := entry.Message.ID()
		msgKey := redisKeys.System{}.Message(msgID)
		historyKey := redisKeys.System{}.MessageAcknowledgementHistory(msgID)
		state := entry.Message.MessageState()

		// Single call to resolveUnackAction per entry. Its action drives
		// the ARGV; both action and cause are captured into `precompute`
		// for UnackResult population. No recomputation happens after the
		// Lua returns.
		action, deadLetterCause := resolveUnackAction(entry.Message, entry.Cause)
		precompute[i] = unackPrecompute{Action: action, Cause: deadLetterCause}

		retryAction := ""
		if action != ActionDeadLetter {
			retryAction = strconv.Itoa(int(action))
		}

		deadLetteredAt := ""
		if action == ActionDeadLetter {
			deadLetteredAt = fmt.Sprintf("%d", now)
		}

		messageExpired := "0"
		if entry.Message.IsExpired() || entry.Cause == consumer.CauseTTLExpired {
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

	return luaKeys, staticArgv, precompute
}

// loadMessages loads full message envelopes from Redis by their IDs.
// Missing messages are silently skipped — this is used by
// UnacknowledgeProcessingQueue to recover a dead consumer's in-flight
// messages, and a message that another process already removed from
// Redis is not an error condition.
func (mu *MessageUnacknowledger) loadMessages(ctx context.Context, ids []string) ([]*internalMessage.Envelope, error) {
	messages := make([]*internalMessage.Envelope, 0, len(ids))
	for _, id := range ids {
		msgKey := redisKeys.System{}.Message(id)
		hash, err := redisClient.LoadHash(ctx, msgKey, "message")
		if err != nil {
			mu.log.Debug("message not found during load", "messageID", id)
			continue
		}
		codec := internalMessage.NewCodec()
		env, err := codec.DecodeEnvelope(ctx, hash)
		if err != nil {
			mu.log.Debug("failed to decode message during load", "messageID", id, "error", err)
			continue
		}
		messages = append(messages, env)
	}
	return messages, nil
}
