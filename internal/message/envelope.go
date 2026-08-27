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

	"github.com/weyoss/go-redis-smq/internal/util/cron"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Envelope wraps a ProducibleMessage with state and routing information.
// Tracks message lifecycle and provides scheduling logic.
// This is an internal runtime type, not exposed in the public API.
type Envelope struct {
	producibleMessage *publicmessage.ProducibleMessage
	messageState      *publicmessage.MessageState
	status            publicmessage.MessageStatus
	destinationQueue  *publicqueue.Params
	consumerGroupID   string
}

// NewEnvelope creates a new message envelope.
// Initializes message state with a unique ID and applies any scheduled delay.
func NewEnvelope(message *publicmessage.ProducibleMessage) *Envelope {
	state := publicmessage.NewMessageState()
	if message.ScheduledDelay() != nil {
		state.SetEffectiveScheduledDelay(message.ScheduledDelay().Milliseconds())
	}

	return &Envelope{
		producibleMessage: message,
		messageState:      state,
		status:            publicmessage.StatusNew,
	}
}

// ProducibleMessage returns the original message configuration.
func (e *Envelope) ProducibleMessage() *publicmessage.ProducibleMessage {
	return e.producibleMessage
}

// MessageState returns the message lifecycle state.
func (e *Envelope) MessageState() *publicmessage.MessageState {
	return e.messageState
}

// SetMessageState replaces the message state.
func (e *Envelope) SetMessageState(state *publicmessage.MessageState) *Envelope {
	e.messageState = state
	return e
}

// ID returns the unique message identifier.
func (e *Envelope) ID() string { return e.messageState.ID() }

// Status returns the current message status.
func (e *Envelope) Status() publicmessage.MessageStatus { return e.status }

// SetStatus updates the message status.
func (e *Envelope) SetStatus(status publicmessage.MessageStatus) *Envelope {
	e.status = status
	return e
}

// DestinationQueue returns the resolved destination queue.
func (e *Envelope) DestinationQueue() *publicqueue.Params {
	return e.destinationQueue
}

// SetDestinationQueue sets the destination queue (called once during routing).
func (e *Envelope) SetDestinationQueue(q *publicqueue.Params) *Envelope {
	e.destinationQueue = q
	return e
}

// ConsumerGroupID returns the consumer group identifier.
func (e *Envelope) ConsumerGroupID() string { return e.consumerGroupID }

// SetConsumerGroupID sets the consumer group for load balancing.
func (e *Envelope) SetConsumerGroupID(id string) *Envelope {
	e.consumerGroupID = id
	return e
}

// GetSetExpired checks if the message TTL has elapsed and marks it expired.
// Returns true if the message has expired.
func (e *Envelope) GetSetExpired() bool {
	ttl := e.producibleMessage.TTL()
	if ttl <= 0 {
		return false
	}

	elapsed := time.Since(e.producibleMessage.CreatedAt())
	if elapsed >= ttl {
		e.messageState.SetExpired(true)
		return true
	}
	return false
}

// IsExpired checks if the message has exceeded its TTL without modifying state.
func (e *Envelope) IsExpired() bool {
	ttl := e.producibleMessage.TTL()
	if ttl <= 0 {
		return false
	}
	return time.Since(e.producibleMessage.CreatedAt()) >= ttl
}

// HasRetryThresholdExceeded checks if retry attempts have been exhausted.
func (e *Envelope) HasRetryThresholdExceeded() bool {
	return e.messageState.Attempts() >= e.producibleMessage.RetryThreshold()
}

// HasNextDelay reports whether the message has a scheduled delay pending.
func (e *Envelope) HasNextDelay() bool {
	return e.messageState.HasDelay()
}

// IsSchedulable reports whether the message can be scheduled for future delivery.
func (e *Envelope) IsSchedulable() bool {
	return e.HasNextDelay() || e.IsPeriodic()
}

// IsPeriodic reports whether the message repeats on a schedule.
func (e *Envelope) IsPeriodic() bool {
	return e.producibleMessage.ScheduledCron() != "" ||
		e.producibleMessage.ScheduledRepeat() > 0
}

// NextScheduledTimestamp calculates the next delivery timestamp in Unix milliseconds.
// Returns 0 if the message is not schedulable or the schedule has ended.
//
// Scheduling priority (matches TypeScript):
//  1. Delay: now + effectiveScheduledDelay (one-time)
//  2. CRON + Repeat: CRON triggers repeat cycles; within a cycle, repeats use period
//  3. CRON only: next CRON time
//  4. Repeat only: first immediate, subsequent delayed by period
func (e *Envelope) NextScheduledTimestamp() int64 {
	if !e.IsSchedulable() {
		return 0
	}

	state := e.messageState
	now := time.Now().UnixMilli()

	// 1. Delay takes precedence — one-time scheduled delivery
	delay := state.EffectiveScheduledDelay()
	if delay > 0 {
		state.ClearEffectiveScheduledDelay()
		return now + delay
	}

	msg := e.producibleMessage
	cronExpr := msg.ScheduledCron()
	repeatLimit := msg.ScheduledRepeat()

	// No periodic scheduling defined
	if cronExpr == "" && repeatLimit == 0 {
		return 0
	}

	// 2. Calculate next CRON timestamp
	var cronTimestamp int64
	if cronExpr != "" {
		if schedule, err := cron.ParseCron(cronExpr); err == nil {
			next := schedule.NextTick(time.Now())
			if !next.IsZero() {
				cronTimestamp = next.UnixMilli()
			}
		}
	}

	// 3. Calculate next Repeat timestamp
	var repeatTimestamp int64
	currentRepeatCount := state.ScheduledRepeatCount()
	if currentRepeatCount < repeatLimit {
		isCronFired := state.ScheduledCronFired()
		// For CRON + REPEAT: REPEAT is active only after the first CRON tick
		if cronExpr == "" || isCronFired {
			period := msg.ScheduledRepeatPeriod()
			if period != nil {
				if cronExpr != "" {
					// CRON + REPEAT: all repeats are delayed by the period
					repeatTimestamp = now + period.Milliseconds()
				} else {
					// REPEAT only: first repeat is immediate, subsequent are delayed
					if currentRepeatCount > 0 {
						repeatTimestamp = now + period.Milliseconds()
					} else {
						repeatTimestamp = now
					}
				}
			}
		}
	}

	// 4. Determine the final timestamp and update state accordingly
	if cronTimestamp > 0 && repeatTimestamp > 0 {
		if repeatTimestamp < cronTimestamp {
			state.IncrScheduledRepeatCount()
			return repeatTimestamp
		}
		// CRON tick takes precedence and resets the repeat cycle
		state.ResetScheduledRepeatCount()
		state.SetScheduledCronFired(true)
		return cronTimestamp
	}

	if cronTimestamp > 0 {
		state.ResetScheduledRepeatCount()
		state.SetScheduledCronFired(true)
		return cronTimestamp
	}

	if repeatTimestamp > 0 {
		state.IncrScheduledRepeatCount()
		return repeatTimestamp
	}

	// Schedule has ended (e.g., repeat limit reached)
	return 0
}

// ToParams converts the envelope to a serializable params struct.
func (e *Envelope) ToParams() *publicmessage.Params {
	return e.producibleMessage.ToParams(e.destinationQueue, e.consumerGroupID)
}

// ToTransferable converts the envelope to a transferable representation
// suitable for serialization and cross-system transfer.
// Matches TypeScript IMessageTransferable format.
func (e *Envelope) ToTransferable() *publicmessage.Transferable {
	params := e.ToParams()

	return &publicmessage.Transferable{
		ID:                    e.messageState.ID(),
		CreatedAt:             params.CreatedAt,
		TTL:                   params.TTL,
		RetryThreshold:        params.RetryThreshold,
		RetryDelay:            params.RetryDelay,
		ConsumeTimeout:        params.ConsumeTimeout,
		Body:                  params.Body,
		Priority:              params.Priority,
		ScheduledCron:         params.ScheduledCron,
		ScheduledDelay:        params.ScheduledDelay,
		ScheduledRepeatPeriod: params.ScheduledRepeatPeriod,
		ScheduledRepeat:       params.ScheduledRepeat,
		Exchange:              params.Exchange,
		Queue:                 params.Queue,
		DestinationQueue:      params.DestinationQueue,
		ConsumerGroupID:       params.ConsumerGroupID,
		MessageState:          e.messageState.ToTransferable(),
		Status:                e.status,
	}
}
