/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"encoding/json"
	"fmt"

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/util"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// BuildPublishArgs builds the full ARGV array for the publish-message Lua script.
func BuildPublishArgs(envelope *internalMessage.Envelope) []interface{} {
	state := envelope.MessageState()
	producibleMsg := envelope.ProducibleMessage()
	params := envelope.ToParams()
	msgState := state.ToTransferable()

	priority := ""
	if producibleMsg.HasPriority() {
		priority = fmt.Sprintf("%d", producibleMsg.Priority().Int())
	}

	scheduledTimestamp := ""
	if st := state.ScheduledAt(); st != nil {
		scheduledTimestamp = fmt.Sprintf("%d", *st)
	}

	schedulingValues := []interface{}{
		priority,
		scheduledTimestamp,
		publicmessage.StatusScheduled.Int(),
		publicmessage.StatusPending.Int(),
	}

	messageJSON, _ := json.Marshal(params)

	messagePropertyValues := []interface{}{
		envelope.ID(),
		envelope.Status().Int(),
		string(messageJSON),
		util.OrEmptyInt64(msgState.ScheduledAt),
		util.OrEmptyInt64(msgState.PublishedAt),
		util.OrEmptyInt64(msgState.ProcessingStartedAt),
		util.OrEmptyInt64(msgState.DeadLetteredAt),
		util.OrEmptyInt64(msgState.AcknowledgedAt),
		util.OrEmptyInt64(msgState.UnacknowledgedAt),
		util.OrEmptyInt64(msgState.LastUnacknowledgedAt),
		util.OrEmptyInt64(msgState.LastScheduledAt),
		util.OrEmptyInt64(msgState.RequeuedAt),
		msgState.RequeueCount,
		util.OrEmptyInt64(msgState.LastRequeuedAt),
		util.OrEmptyInt64(msgState.LastRetriedAttemptAt),
		util.BoolToInt(msgState.ScheduledCronFired),
		msgState.Attempts,
		msgState.ScheduledRepeatCount,
		util.BoolToInt(msgState.Expired),
		msgState.EffectiveScheduledDelay,
		msgState.ScheduledTimes,
		msgState.ScheduledMessageParentID,
		msgState.RequeuedMessageParentID,
		util.OrEmptyInt64(msgState.LastProcessedAt),
	}

	args := make([]interface{}, 0, 67)
	args = append(args, buildQueuePropertyArgs()...)
	args = append(args, schedulingValues...)
	args = append(args, buildMessagePropertyKeys()...)
	args = append(args, messagePropertyValues...)
	args = append(args, envelope.ConsumerGroupID())
	args = append(args, "")

	return args
}

// buildQueuePropertyArgs returns the static queue property constants for the publish-message Lua script.
// These are the first 13 ARGV values used by publish-message.lua shared procedure.
func buildQueuePropertyArgs() []interface{} {
	return []interface{}{
		qSchema.QueueFieldType.Key(),
		qSchema.QueueFieldMessagesCount.Key(),
		qSchema.QueueFieldPendingMessagesCount.Key(),
		qSchema.QueueFieldScheduledMessagesCount.Key(),
		queue.TypePriority.Int(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		qSchema.QueueFieldLockID.Key(),
		queue.StateActive.Int(),
		queue.StatePaused.Int(),
		queue.StateStopped.Int(),
		queue.StateLocked.Int(),
	}
}

// buildMessagePropertyKeys returns the static message property keys for the publish-message Lua script.
// These are the 24 ARGV values used by publish-message.lua shared procedure.
func buildMessagePropertyKeys() []interface{} {
	return []interface{}{
		internalMessage.MessageFieldID.Key(),
		internalMessage.MessageFieldStatus.Key(),
		internalMessage.MessageFieldMessage.Key(),
		internalMessage.MessageFieldScheduledAt.Key(),
		internalMessage.MessageFieldPublishedAt.Key(),
		internalMessage.MessageFieldProcessingStartedAt.Key(),
		internalMessage.MessageFieldDeadLetteredAt.Key(),
		internalMessage.MessageFieldAcknowledgedAt.Key(),
		internalMessage.MessageFieldUnacknowledgedAt.Key(),
		internalMessage.MessageFieldLastUnacknowledgedAt.Key(),
		internalMessage.MessageFieldLastScheduledAt.Key(),
		internalMessage.MessageFieldRequeuedAt.Key(),
		internalMessage.MessageFieldRequeueCount.Key(),
		internalMessage.MessageFieldLastRequeuedAt.Key(),
		internalMessage.MessageFieldLastRetriedAttemptAt.Key(),
		internalMessage.MessageFieldScheduledCronFired.Key(),
		internalMessage.MessageFieldAttempts.Key(),
		internalMessage.MessageFieldScheduledRepeatCount.Key(),
		internalMessage.MessageFieldExpired.Key(),
		internalMessage.MessageFieldEffectiveScheduledDelay.Key(),
		internalMessage.MessageFieldScheduledTimes.Key(),
		internalMessage.MessageFieldScheduledMessageParentID.Key(),
		internalMessage.MessageFieldRequeuedMessageParentID.Key(),
		internalMessage.MessageFieldLastProcessedAt.Key(),
	}
}
