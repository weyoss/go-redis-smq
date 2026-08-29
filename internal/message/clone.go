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
	"time"

	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/message"
)

// CloneMessage creates a deep copy of a MessageEnvelope for requeue operations.
// Matches TypeScript _fromMessage logic:
//   - Creates a new ProducibleMessage with the same properties
//   - Resets scheduled parameters on the new message
//   - Creates a fresh State with a new ID
//   - Sets status to NEW
//   - Preserves destination queue and consumer group
func CloneMessage(source *Envelope) *Envelope {
	return cloneMessage(source, false)
}

// CloneMessageWithState creates a deep copy preserving the existing state.
// Used when rescheduling or retrying a message without resetting its lifecycle.
func CloneMessageWithState(source *Envelope) *Envelope {
	return cloneMessage(source, true)
}

func cloneMessage(source *Envelope, preserveState bool) *Envelope {
	producibleMsg := source.ProducibleMessage()

	// Create new message with same properties
	newMsg := message.New()
	newMsg.SetTTL(producibleMsg.TTL())
	newMsg.SetRetryThreshold(producibleMsg.RetryThreshold())
	newMsg.SetRetryDelay(producibleMsg.RetryDelay())
	newMsg.SetConsumeTimeout(producibleMsg.ConsumeTimeout())
	newMsg.SetBody(producibleMsg.Body())

	// Copy priority if set
	if producibleMsg.HasPriority() {
		newMsg.SetPriority(*producibleMsg.Priority())
	}

	// Copy scheduling parameters
	if cron := producibleMsg.ScheduledCron(); cron != "" {
		newMsg.SetScheduledCron(cron)
	}
	if delay := producibleMsg.ScheduledDelay(); delay != nil {
		newMsg.SetScheduledDelay(*delay)
	}
	if period := producibleMsg.ScheduledRepeatPeriod(); period != nil {
		newMsg.SetScheduledRepeatPeriod(*period)
	}
	newMsg.SetScheduledRepeat(producibleMsg.ScheduledRepeat())

	// Copy exchange configuration
	if ex := producibleMsg.Exchange(); ex != nil {
		switch ex.Type() {
		case exchange.TypeDirect:
			newMsg.SetDirectExchange(ex)
		case exchange.TypeFanout:
			newMsg.SetFanoutExchange(ex)
		case exchange.TypeTopic:
			newMsg.SetTopicExchange(ex)
		}
	}

	// Copy routing key if set
	if rk := producibleMsg.ExchangeRoutingKey(); rk != "" {
		newMsg.SetExchangeRoutingKey(rk)
	}

	// Copy queue if direct queue delivery
	if q := producibleMsg.Queue(); q != nil {
		newMsg.SetQueue(q)
	}

	// Create new envelope with fresh state
	envelope := NewEnvelope(newMsg)

	// Set destination and consumer group
	envelope.SetDestinationQueue(source.DestinationQueue())
	if cgID := source.ConsumerGroupID(); cgID != "" {
		envelope.SetConsumerGroupID(cgID)
	}

	if preserveState {
		// Preserve the original state but with a new ID
		originalState := source.MessageState()
		transferable := originalState.ToTransferable()

		cloneState := envelope.MessageState()
		cloneState.SetAttempts(transferable.Attempts)
		cloneState.SetScheduledRepeatCount(transferable.ScheduledRepeatCount)
		cloneState.SetScheduledTimes(transferable.ScheduledTimes)
		cloneState.SetRequeueCount(transferable.RequeueCount)
		cloneState.SetExpired(transferable.Expired)
		cloneState.SetScheduledCronFired(transferable.ScheduledCronFired)

		if transferable.EffectiveScheduledDelay > 0 {
			cloneState.SetEffectiveScheduledDelay(transferable.EffectiveScheduledDelay)
		}

		// Copy parent relationships
		if transferable.ScheduledMessageParentID != "" {
			cloneState.SetScheduledMessageParentID(transferable.ScheduledMessageParentID)
		}
		if transferable.RequeuedMessageParentID != "" {
			cloneState.SetRequeuedMessageParentID(transferable.RequeuedMessageParentID)
		}

		// Preserve status
		envelope.SetStatus(source.Status())
	} else {
		// Fresh state for requeue
		envelope.SetStatus(message.StatusNew)

		// Mark the requeue parent relationship
		state := envelope.MessageState()
		state.SetRequeuedMessageParentID(source.ID())
		state.SetPublishedAt(time.Now().UnixMilli())
	}

	return envelope
}
