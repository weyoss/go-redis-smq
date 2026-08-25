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

	"github.com/google/uuid"
)

// MessageState tracks the lifecycle of a message through the system.
//
// It holds timestamps, counters, parent relationships, and scheduling
// information for a message. The state is serialized to a Redis hash using
// integer field keys defined in the internal message schema.
type MessageState struct {
	id                       string
	scheduledAt              *time.Time
	publishedAt              *time.Time
	requeuedAt               *time.Time
	processingStartedAt      *time.Time
	acknowledgedAt           *time.Time
	unacknowledgedAt         *time.Time
	deadLetteredAt           *time.Time
	lastRequeuedAt           *time.Time
	lastUnacknowledgedAt     *time.Time
	lastScheduledAt          *time.Time
	lastRetriedAttemptAt     *time.Time
	lastProcessedAt          *time.Time
	scheduledCronFired       bool
	attempts                 int
	scheduledRepeatCount     int
	requeueCount             int
	expired                  bool
	effectiveScheduledDelay  int64
	scheduledTimes           int
	scheduledMessageParentID string
	requeuedMessageParentID  string
}

// NewMessageState creates a new message state with a unique ID.
func NewMessageState() *MessageState {
	return &MessageState{id: uuid.New().String()}
}

// ID returns the unique message identifier.
func (ms *MessageState) ID() string { return ms.id }

// SetID sets the unique message identifier.
func (ms *MessageState) SetID(id string) { ms.id = id }

// Attempts returns the number of consumption attempts.
func (ms *MessageState) Attempts() int { return ms.attempts }

// SetAttempts sets the number of consumption attempts.
func (ms *MessageState) SetAttempts(n int) { ms.attempts = n }

// IncrAttempts increments the consumption attempt counter by one.
func (ms *MessageState) IncrAttempts() { ms.attempts++ }

// HasDelay reports whether the message has a scheduled delay pending.
func (ms *MessageState) HasDelay() bool { return ms.effectiveScheduledDelay > 0 }

// EffectiveScheduledDelay returns the current effective scheduled delay in
// milliseconds.
func (ms *MessageState) EffectiveScheduledDelay() int64 { return ms.effectiveScheduledDelay }

// SetEffectiveScheduledDelay sets the effective scheduled delay in milliseconds.
func (ms *MessageState) SetEffectiveScheduledDelay(delay int64) {
	ms.effectiveScheduledDelay = delay
}

// ClearEffectiveScheduledDelay resets the effective scheduled delay to zero.
func (ms *MessageState) ClearEffectiveScheduledDelay() {
	ms.effectiveScheduledDelay = 0
}

// ScheduledCronFired reports whether the CRON trigger has fired.
func (ms *MessageState) ScheduledCronFired() bool { return ms.scheduledCronFired }

// SetScheduledCronFired sets whether the CRON trigger has fired.
func (ms *MessageState) SetScheduledCronFired(fired bool) {
	ms.scheduledCronFired = fired
}

// ScheduledRepeatCount returns the current repeat count.
func (ms *MessageState) ScheduledRepeatCount() int { return ms.scheduledRepeatCount }

// SetScheduledRepeatCount sets the current repeat count.
func (ms *MessageState) SetScheduledRepeatCount(n int) {
	ms.scheduledRepeatCount = n
}

// IncrScheduledRepeatCount increments the repeat count by one.
func (ms *MessageState) IncrScheduledRepeatCount() {
	ms.scheduledRepeatCount++
}

// ResetScheduledRepeatCount resets the repeat count to zero.
func (ms *MessageState) ResetScheduledRepeatCount() {
	ms.scheduledRepeatCount = 0
}

// Expired reports whether the message has expired.
func (ms *MessageState) Expired() bool { return ms.expired }

// SetExpired marks the message as expired or not expired.
func (ms *MessageState) SetExpired(expired bool) { ms.expired = expired }

// IsExpired checks whether the message TTL has elapsed.
func (ms *MessageState) IsExpired(ttl time.Duration, createdAt time.Time) bool {
	if ttl <= 0 {
		return false
	}
	return time.Since(createdAt) >= ttl
}

// ScheduledTimes returns the number of times the message has been scheduled.
func (ms *MessageState) ScheduledTimes() int { return ms.scheduledTimes }

// SetScheduledTimes sets the number of times the message has been scheduled.
func (ms *MessageState) SetScheduledTimes(n int) { ms.scheduledTimes = n }

// IncrScheduledTimes increments the scheduled times counter by one.
func (ms *MessageState) IncrScheduledTimes() { ms.scheduledTimes++ }

// RequeueCount returns the number of times the message has been requeued.
func (ms *MessageState) RequeueCount() int { return ms.requeueCount }

// SetRequeueCount sets the number of times the message has been requeued.
func (ms *MessageState) SetRequeueCount(n int) { ms.requeueCount = n }

// ScheduledMessageParentID returns the parent scheduled message ID.
func (ms *MessageState) ScheduledMessageParentID() string {
	return ms.scheduledMessageParentID
}

// SetScheduledMessageParentID sets the parent scheduled message ID.
func (ms *MessageState) SetScheduledMessageParentID(id string) {
	ms.scheduledMessageParentID = id
}

// RequeuedMessageParentID returns the parent requeued message ID.
func (ms *MessageState) RequeuedMessageParentID() string {
	return ms.requeuedMessageParentID
}

// SetRequeuedMessageParentID sets the parent requeued message ID.
func (ms *MessageState) SetRequeuedMessageParentID(id string) {
	ms.requeuedMessageParentID = id
}

// ScheduledAt returns the scheduled delivery timestamp in Unix milliseconds,
// or nil if not scheduled.
func (ms *MessageState) ScheduledAt() *int64 {
	if ms.scheduledAt == nil {
		return nil
	}
	v := ms.scheduledAt.UnixMilli()
	return &v
}

// PublishedAt returns the publish timestamp in Unix milliseconds, or nil if
// not published.
func (ms *MessageState) PublishedAt() *int64 {
	if ms.publishedAt == nil {
		return nil
	}
	v := ms.publishedAt.UnixMilli()
	return &v
}

// RequeuedAt returns the first requeue timestamp in Unix milliseconds, or nil
// if not requeued.
func (ms *MessageState) RequeuedAt() *int64 {
	if ms.requeuedAt == nil {
		return nil
	}
	v := ms.requeuedAt.UnixMilli()
	return &v
}

// ProcessingStartedAt returns the processing start timestamp in Unix
// milliseconds, or nil if processing has not started.
func (ms *MessageState) ProcessingStartedAt() *int64 {
	if ms.processingStartedAt == nil {
		return nil
	}
	v := ms.processingStartedAt.UnixMilli()
	return &v
}

// AcknowledgedAt returns the acknowledgment timestamp in Unix milliseconds,
// or nil if not acknowledged.
func (ms *MessageState) AcknowledgedAt() *int64 {
	if ms.acknowledgedAt == nil {
		return nil
	}
	v := ms.acknowledgedAt.UnixMilli()
	return &v
}

// UnacknowledgedAt returns the most recent unacknowledgment timestamp in Unix
// milliseconds, or nil if not unacknowledged.
func (ms *MessageState) UnacknowledgedAt() *int64 {
	if ms.unacknowledgedAt == nil {
		return nil
	}
	v := ms.unacknowledgedAt.UnixMilli()
	return &v
}

// DeadLetteredAt returns the dead-letter timestamp in Unix milliseconds, or
// nil if not dead-lettered.
func (ms *MessageState) DeadLetteredAt() *int64 {
	if ms.deadLetteredAt == nil {
		return nil
	}
	v := ms.deadLetteredAt.UnixMilli()
	return &v
}

// LastRequeuedAt returns the most recent requeue timestamp in Unix
// milliseconds, or nil if not requeued.
func (ms *MessageState) LastRequeuedAt() *int64 {
	if ms.lastRequeuedAt == nil {
		return nil
	}
	v := ms.lastRequeuedAt.UnixMilli()
	return &v
}

// LastUnacknowledgedAt returns the previous unacknowledgment timestamp in Unix
// milliseconds, or nil if not set.
func (ms *MessageState) LastUnacknowledgedAt() *int64 {
	if ms.lastUnacknowledgedAt == nil {
		return nil
	}
	v := ms.lastUnacknowledgedAt.UnixMilli()
	return &v
}

// LastScheduledAt returns the last scheduled timestamp in Unix milliseconds,
// or nil if not scheduled.
func (ms *MessageState) LastScheduledAt() *int64 {
	if ms.lastScheduledAt == nil {
		return nil
	}
	v := ms.lastScheduledAt.UnixMilli()
	return &v
}

// LastRetriedAttemptAt returns the last retry attempt timestamp in Unix
// milliseconds, or nil if not retried.
func (ms *MessageState) LastRetriedAttemptAt() *int64 {
	if ms.lastRetriedAttemptAt == nil {
		return nil
	}
	v := ms.lastRetriedAttemptAt.UnixMilli()
	return &v
}

// LastProcessedAt returns the last processed timestamp in Unix milliseconds,
// or nil if not processed.
func (ms *MessageState) LastProcessedAt() *int64 {
	if ms.lastProcessedAt == nil {
		return nil
	}
	v := ms.lastProcessedAt.UnixMilli()
	return &v
}

// SetPublishedAt sets the publish timestamp from Unix milliseconds.
func (ms *MessageState) SetPublishedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.publishedAt = &t
}

// SetScheduledAt sets the scheduled timestamp from Unix milliseconds.
func (ms *MessageState) SetScheduledAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.scheduledAt = &t
}

// SetRequeuedAt sets the first requeue timestamp from Unix milliseconds.
func (ms *MessageState) SetRequeuedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.requeuedAt = &t
}

// SetLastRequeuedAt sets the most recent requeue timestamp from Unix
// milliseconds.
func (ms *MessageState) SetLastRequeuedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastRequeuedAt = &t
}

// SetAcknowledgedAt sets the acknowledgment timestamp from Unix milliseconds.
func (ms *MessageState) SetAcknowledgedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.acknowledgedAt = &t
}

// SetUnacknowledgedAt sets the most recent unacknowledgment timestamp from
// Unix milliseconds.
func (ms *MessageState) SetUnacknowledgedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.unacknowledgedAt = &t
}

// SetLastUnacknowledgedAt sets the previous unacknowledgment timestamp from
// Unix milliseconds.
func (ms *MessageState) SetLastUnacknowledgedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastUnacknowledgedAt = &t
}

// SetDeadLetteredAt sets the dead-letter timestamp from Unix milliseconds.
func (ms *MessageState) SetDeadLetteredAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.deadLetteredAt = &t
}

// SetProcessingStartedAt sets the processing start timestamp from Unix
// milliseconds.
func (ms *MessageState) SetProcessingStartedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.processingStartedAt = &t
}

// SetLastProcessedAt sets the last processed timestamp from Unix milliseconds.
func (ms *MessageState) SetLastProcessedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastProcessedAt = &t
}

// SetLastScheduledAt sets the last scheduled timestamp from Unix milliseconds.
func (ms *MessageState) SetLastScheduledAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastScheduledAt = &t
}

// SetLastRetriedAttemptAt sets the last retry attempt timestamp from Unix
// milliseconds.
func (ms *MessageState) SetLastRetriedAttemptAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastRetriedAttemptAt = &t
}

// MarkPublished records the current time as the publish timestamp.
func (ms *MessageState) MarkPublished() {
	now := time.Now()
	ms.publishedAt = &now
}

// ToTransferable converts the state to a transferable representation.
func (ms *MessageState) ToTransferable() StateTransferable {
	s := StateTransferable{
		UUID:                     ms.id,
		ScheduledCronFired:       ms.scheduledCronFired,
		Attempts:                 ms.attempts,
		ScheduledRepeatCount:     ms.scheduledRepeatCount,
		RequeueCount:             ms.requeueCount,
		Expired:                  ms.expired,
		EffectiveScheduledDelay:  ms.effectiveScheduledDelay,
		ScheduledTimes:           ms.scheduledTimes,
		ScheduledMessageParentID: ms.scheduledMessageParentID,
		RequeuedMessageParentID:  ms.requeuedMessageParentID,
	}
	if ms.scheduledAt != nil {
		v := ms.scheduledAt.UnixMilli()
		s.ScheduledAt = &v
	}
	if ms.publishedAt != nil {
		v := ms.publishedAt.UnixMilli()
		s.PublishedAt = &v
	}
	if ms.requeuedAt != nil {
		v := ms.requeuedAt.UnixMilli()
		s.RequeuedAt = &v
	}
	if ms.processingStartedAt != nil {
		v := ms.processingStartedAt.UnixMilli()
		s.ProcessingStartedAt = &v
	}
	if ms.acknowledgedAt != nil {
		v := ms.acknowledgedAt.UnixMilli()
		s.AcknowledgedAt = &v
	}
	if ms.unacknowledgedAt != nil {
		v := ms.unacknowledgedAt.UnixMilli()
		s.UnacknowledgedAt = &v
	}
	if ms.deadLetteredAt != nil {
		v := ms.deadLetteredAt.UnixMilli()
		s.DeadLetteredAt = &v
	}
	if ms.lastRequeuedAt != nil {
		v := ms.lastRequeuedAt.UnixMilli()
		s.LastRequeuedAt = &v
	}
	if ms.lastUnacknowledgedAt != nil {
		v := ms.lastUnacknowledgedAt.UnixMilli()
		s.LastUnacknowledgedAt = &v
	}
	if ms.lastScheduledAt != nil {
		v := ms.lastScheduledAt.UnixMilli()
		s.LastScheduledAt = &v
	}
	if ms.lastRetriedAttemptAt != nil {
		v := ms.lastRetriedAttemptAt.UnixMilli()
		s.LastRetriedAttemptAt = &v
	}
	if ms.lastProcessedAt != nil {
		v := ms.lastProcessedAt.UnixMilli()
		s.LastProcessedAt = &v
	}
	return s
}
