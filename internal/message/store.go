/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	mSchema "github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Store provides Redis-backed message persistence and lifecycle operations.
type Store struct {
	envelopeCodec *EnvelopeCodec
	stateCodec    *StateCodec
}

// NewStore creates a new Store with the given codecs.
// Passing nil will use default codecs.
func NewStore(envelopeCodec *EnvelopeCodec, stateCodec *StateCodec) *Store {
	if envelopeCodec == nil {
		envelopeCodec = NewEnvelopeCodec()
	}
	if stateCodec == nil {
		stateCodec = NewStateCodec()
	}
	return &Store{
		envelopeCodec: envelopeCodec,
		stateCodec:    stateCodec,
	}
}

// GetStatus returns the current status of a message.
func (s *Store) GetStatus(ctx context.Context, messageID string) (msg.MessageStatus, error) {
	statusStr, err := redis.LoadHashField(ctx,
		keys.System{}.Message(messageID),
		mSchema.MessageFieldStatus.Key(),
		"message status",
	)
	if err != nil {
		return 0, msg.ErrNotFound
	}

	status, err := strconv.Atoi(statusStr)
	if err != nil {
		return 0, fmt.Errorf("parse message status: %w", err)
	}

	return msg.MessageStatus(status), nil
}

// GetState returns the lifecycle state of a message.
func (s *Store) GetState(ctx context.Context, messageID string) (*msg.MessageState, error) {
	hash, err := redis.LoadHash(ctx, keys.System{}.Message(messageID), "message state")
	if err != nil {
		return nil, msg.ErrNotFound
	}
	return s.stateCodec.DecodeHash(ctx, hash)
}

// GetMessage returns a single message envelope by its ID.
func (s *Store) GetMessage(ctx context.Context, messageID string) (*Envelope, error) {
	hash, err := redis.LoadHash(ctx, keys.System{}.Message(messageID), "message")
	if err != nil {
		return nil, msg.ErrNotFound
	}
	return s.envelopeCodec.DecodeHash(ctx, hash)
}

// GetMessages returns multiple message envelopes. Missing messages are skipped.
func (s *Store) GetMessages(ctx context.Context, messageIDs []string) ([]*Envelope, error) {
	envelopes := make([]*Envelope, 0, len(messageIDs))
	for _, id := range messageIDs {
		env, err := s.GetMessage(ctx, id)
		if err != nil {
			if errors.Is(err, msg.ErrNotFound) {
				continue
			}
			return nil, err
		}
		envelopes = append(envelopes, env)
	}
	return envelopes, nil
}

// DeleteMessages deletes one or more messages. Returns summary statistics.
func (s *Store) DeleteMessages(ctx context.Context, messageIDs []string, lockID string) (*msg.DeleteResponse, error) {
	response := &msg.DeleteResponse{Status: msg.DeleteStatusNotDeleted}

	if len(messageIDs) == 0 {
		response.Status = msg.DeleteStatusOK
		return response, nil
	}

	messages, err := s.GetMessages(ctx, messageIDs)
	if err != nil {
		return nil, err
	}

	response.Stats.NotFound = len(messageIDs) - len(messages)
	response.Stats.Processed = response.Stats.NotFound

	if len(messages) == 0 {
		return response, nil
	}

	type messageGroup struct {
		queue    *publicqueue.QueueParams
		messages []*Envelope
	}

	groups := make(map[string]map[string]*messageGroup)
	for _, env := range messages {
		destQueue := env.DestinationQueue()
		if destQueue == nil {
			continue
		}
		queueID := destQueue.NS() + ":" + destQueue.Name()
		cgID := env.ConsumerGroupID()
		if cgID == "" {
			cgID = "_"
		}
		if groups[queueID] == nil {
			groups[queueID] = make(map[string]*messageGroup)
		}
		if groups[queueID][cgID] == nil {
			groups[queueID][cgID] = &messageGroup{queue: destQueue}
		}
		groups[queueID][cgID].messages = append(groups[queueID][cgID].messages, env)
	}

	for _, cgMap := range groups {
		for _, group := range cgMap {
			stats, err := s.deleteMessageGroup(ctx, group.queue, group.messages, lockID)
			if err != nil {
				return nil, err
			}
			response.Stats.Processed += stats.Processed
			response.Stats.Success += stats.Success
			response.Stats.NotFound += stats.NotFound
			response.Stats.InProcess += stats.InProcess
		}
	}

	if response.Stats.Processed == response.Stats.Success && response.Stats.Processed > 0 {
		response.Status = msg.DeleteStatusOK
	} else if response.Stats.Success > 0 {
		response.Status = msg.DeleteStatusPartialSuccess
	}

	return response, nil
}

// deleteMessageGroup deletes messages for a specific queue/group.
func (s *Store) deleteMessageGroup(
	ctx context.Context,
	queue *publicqueue.QueueParams,
	messages []*Envelope,
	lockID string,
) (*msg.DeleteStats, error) {
	qKey := keys.Queue{Namespace: queue.NS(), Name: queue.Name()}

	luaKeys := []string{
		qKey.Properties(),
		qKey.Published(),
		qKey.Pending(),
		qKey.Priority(),
		qKey.Scheduled(),
		qKey.Acknowledged(),
		qKey.DeadLetter(),
		qKey.Delayed(),
		qKey.Requeued(),
	}

	luaArgs := []interface{}{
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldMessagesCount.Key(),
		qSchema.QueueFieldAcknowledgedMessagesCount.Key(),
		qSchema.QueueFieldDeadLetteredMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldScheduledMessagesCount.Key(),
		qSchema.QueueFieldDelayedMessagesCount.Key(),
		qSchema.QueueFieldRequeuedMessagesCount.Key(),
		publicqueue.TypePriority.Int(),
		publicqueue.TypeLIFO.Int(),
		publicqueue.TypeFIFO.Int(),
		mSchema.MessageFieldStatus.Key(),
		msg.StatusProcessing.Int(),
		msg.StatusAcknowledged.Int(),
		msg.StatusPending.Int(),
		msg.StatusScheduled.Int(),
		msg.StatusDeadLettered.Int(),
		msg.StatusUnackDelaying.Int(),
		msg.StatusUnackRequeuing.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		publicqueue.StateLocked.Int(),
		qSchema.QueueFieldLockID.Key(),
		lockID,
	}

	for _, env := range messages {
		luaKeys = append(luaKeys,
			keys.System{}.Message(env.ID()),
			keys.System{}.MessageAcknowledgementHistory(env.ID()),
		)
		luaArgs = append(luaArgs, env.ID())
	}

	reply, err := redis.Eval(ctx, scripts.DeleteMessage, luaKeys, luaArgs...)
	if err != nil {
		return nil, fmt.Errorf("delete message script: %w", err)
	}

	if replyStr, ok := reply.(string); ok {
		switch replyStr {
		case "QUEUE_LOCKED":
			return nil, publicqueue.ErrLocked
		case "QUEUE_NOT_FOUND":
			return nil, publicqueue.ErrNotFound
		default:
			return nil, fmt.Errorf("unexpected script reply: %s", replyStr)
		}
	}

	arr, ok := reply.([]interface{})
	if !ok || len(arr) < 4 {
		return nil, fmt.Errorf("unexpected script reply: %v", reply)
	}

	return &msg.DeleteStats{
		Processed: toInt(arr[0]),
		Success:   toInt(arr[1]),
		NotFound:  toInt(arr[2]),
		InProcess: toInt(arr[3]),
	}, nil
}

// RequeueMessage creates a new message from an acknowledged or dead-lettered
// message and returns the new message ID.
func (s *Store) RequeueMessage(ctx context.Context, messageID string) (string, error) {
	original, err := s.GetMessage(ctx, messageID)
	if err != nil {
		return "", err
	}

	if !original.Status().IsRequeuable() {
		return "", msg.ErrNotRequeuable
	}

	destQueue := original.DestinationQueue()
	if destQueue == nil {
		return "", fmt.Errorf("requeue: message has no destination queue")
	}

	ts := time.Now().UnixMilli()

	clone := CloneMessage(original)
	clone.ProducibleMessage().ResetScheduledParams()

	cloneState := clone.MessageState()
	cloneState.SetPublishedAt(ts)
	cloneState.SetRequeuedMessageParentID(messageID)

	newID := clone.ID()
	consumerGroupID := original.ConsumerGroupID()

	qKey := keys.Queue{Namespace: destQueue.NS(), Name: destQueue.Name()}

	luaKeys := []string{
		qKey.Properties(),
		qKey.Priority(),
		qKey.Pending(),
		qKey.Published(),
		qKey.Scheduled(),
		qKey.ConsumerGroups(),
		keys.System{}.Message(messageID),
		keys.System{}.Message(newID),
	}

	priority := ""
	if clone.ProducibleMessage().HasPriority() {
		priority = fmt.Sprintf("%d", clone.ProducibleMessage().Priority().Int())
	}

	newMessageJSON, _ := json.Marshal(clone.ToTransferable())

	requeuedAt := ts
	if at := original.MessageState().RequeuedAt(); at != nil {
		requeuedAt = *at
	}

	argv := []interface{}{
		// Queue Property Constants (ARGV[1-13])
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldScheduledMessagesCount.Key(),
		publicqueue.TypePriority.Int(),
		publicqueue.TypeLIFO.Int(),
		publicqueue.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		qSchema.QueueFieldLockID.Key(),
		publicqueue.StateActive.Int(),
		publicqueue.StatePaused.Int(),
		publicqueue.StateStopped.Int(),
		publicqueue.StateLocked.Int(),

		// Message Status Constants (ARGV[14-15])
		msg.StatusScheduled.Int(),
		msg.StatusPending.Int(),

		// Message Property Keys (ARGV[16-39]) - 24 keys
		mSchema.MessageFieldID.Key(),
		mSchema.MessageFieldStatus.Key(),
		mSchema.MessageFieldMessage.Key(),
		mSchema.MessageFieldScheduledAt.Key(),
		mSchema.MessageFieldPublishedAt.Key(),
		mSchema.MessageFieldProcessingStartedAt.Key(),
		mSchema.MessageFieldDeadLetteredAt.Key(),
		mSchema.MessageFieldAcknowledgedAt.Key(),
		mSchema.MessageFieldUnacknowledgedAt.Key(),
		mSchema.MessageFieldLastUnacknowledgedAt.Key(),
		mSchema.MessageFieldLastScheduledAt.Key(),
		mSchema.MessageFieldRequeuedAt.Key(),
		mSchema.MessageFieldRequeueCount.Key(),
		mSchema.MessageFieldLastRequeuedAt.Key(),
		mSchema.MessageFieldLastRetriedAttemptAt.Key(),
		mSchema.MessageFieldScheduledCronFired.Key(),
		mSchema.MessageFieldAttempts.Key(),
		mSchema.MessageFieldScheduledRepeatCount.Key(),
		mSchema.MessageFieldExpired.Key(),
		mSchema.MessageFieldEffectiveScheduledDelay.Key(),
		mSchema.MessageFieldScheduledTimes.Key(),
		mSchema.MessageFieldScheduledMessageParentID.Key(),
		mSchema.MessageFieldRequeuedMessageParentID.Key(),
		mSchema.MessageFieldLastProcessedAt.Key(),

		"", // ARGV[40] operationLockId

		newID,
		string(newMessageJSON),
		priority,
		ts,
		requeuedAt,
		ts,
		consumerGroupID,
	}

	reply, err := redis.Eval(ctx, scripts.RequeueMessage, luaKeys, argv...)
	if err != nil {
		return "", fmt.Errorf("requeue message: %w", err)
	}

	n, ok := reply.(int64)
	if !ok {
		replyStr, _ := reply.(string)
		return "", fmt.Errorf("requeue message: script error: %s", replyStr)
	}

	if n == 0 {
		return "", msg.ErrNotFound
	}

	return newID, nil
}

// GetUnacknowledgmentHistory returns the unacknowledgment history for a message.
func (s *Store) GetUnacknowledgmentHistory(ctx context.Context, messageID string) ([]string, error) {
	key := keys.System{}.Message(messageID)
	exists, err := redis.Client().Exists(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("check message exists: %w", err)
	}
	if exists == 0 {
		return nil, msg.ErrNotFound
	}

	historyKey := keys.System{}.MessageAcknowledgementHistory(messageID)
	records, err := redis.Client().LRange(ctx, historyKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("get unacknowledgment history: %w", err)
	}

	return records, nil
}

// toInt converts an interface{} (typically int64) to int.
func toInt(v interface{}) int {
	if n, ok := v.(int64); ok {
		return int(n)
	}
	return 0
}
