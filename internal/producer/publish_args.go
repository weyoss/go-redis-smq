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
	"github.com/weyoss/go-redis-smq/internal/message/schema"
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
		qSchema.Type.Key(),
		qSchema.MessagesCount.Key(),
		qSchema.PendingMessagesCount.Key(),
		qSchema.ScheduledMessagesCount.Key(),
		queue.TypePriority.Int(),
		queue.TypeLIFO.Int(),
		queue.TypeFIFO.Int(),
		qSchema.OperationalState.Key(),
		qSchema.LockID.Key(),
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
		schema.ID.Key(),
		schema.Status.Key(),
		schema.Message.Key(),
		schema.ScheduledAt.Key(),
		schema.PublishedAt.Key(),
		schema.ProcessingStartedAt.Key(),
		schema.DeadLetteredAt.Key(),
		schema.AcknowledgedAt.Key(),
		schema.UnacknowledgedAt.Key(),
		schema.LastUnacknowledgedAt.Key(),
		schema.LastScheduledAt.Key(),
		schema.RequeuedAt.Key(),
		schema.RequeueCount.Key(),
		schema.LastRequeuedAt.Key(),
		schema.LastRetriedAttemptAt.Key(),
		schema.ScheduledCronFired.Key(),
		schema.Attempts.Key(),
		schema.ScheduledRepeatCount.Key(),
		schema.Expired.Key(),
		schema.EffectiveScheduledDelay.Key(),
		schema.ScheduledTimes.Key(),
		schema.ScheduledMessageParentID.Key(),
		schema.RequeuedMessageParentID.Key(),
		schema.LastProcessedAt.Key(),
	}
}
