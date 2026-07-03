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
	mSchema "github.com/weyoss/go-redis-smq/internal/message/schema"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/util"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
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
		msg.StatusScheduled.Int(),
		msg.StatusPending.Int(),
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
		q.TypePriority.Int(),
		q.TypeLIFO.Int(),
		q.TypeFIFO.Int(),
		qSchema.QueueFieldOperationalState.Key(),
		qSchema.QueueFieldLockID.Key(),
		q.StateActive.Int(),
		q.StatePaused.Int(),
		q.StateStopped.Int(),
		q.StateLocked.Int(),
	}
}

// buildMessagePropertyKeys returns the static message property keys for the publish-message Lua script.
// These are the 24 ARGV values used by publish-message.lua shared procedure.
func buildMessagePropertyKeys() []interface{} {
	return []interface{}{
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
	}
}
