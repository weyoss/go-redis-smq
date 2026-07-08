/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package msg

import (
	"time"

	"github.com/google/uuid"
)

// MessageState tracks the lifecycle of a message through the system.
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

func NewMessageState() *MessageState {
	return &MessageState{id: uuid.New().String()}
}

// ID returns the unique message identifier.
func (ms *MessageState) ID() string { return ms.id }

func (ms *MessageState) SetID(id string) { ms.id = id }

// Attempts returns the number of consumption attempts.
func (ms *MessageState) Attempts() int { return ms.attempts }

func (ms *MessageState) SetAttempts(n int) { ms.attempts = n }

func (ms *MessageState) IncrAttempts() { ms.attempts++ }

// HasDelay reports whether the message has a scheduled delay.
func (ms *MessageState) HasDelay() bool { return ms.effectiveScheduledDelay > 0 }

// EffectiveScheduledDelay returns the current effective scheduled delay in milliseconds.
func (ms *MessageState) EffectiveScheduledDelay() int64 { return ms.effectiveScheduledDelay }

func (ms *MessageState) SetEffectiveScheduledDelay(delay int64) { ms.effectiveScheduledDelay = delay }

func (ms *MessageState) ClearEffectiveScheduledDelay() { ms.effectiveScheduledDelay = 0 }

// ScheduledCronFired reports whether the CRON trigger has fired.
func (ms *MessageState) ScheduledCronFired() bool { return ms.scheduledCronFired }

func (ms *MessageState) SetScheduledCronFired(fired bool) { ms.scheduledCronFired = fired }

// ScheduledRepeatCount returns the current repeat count.
func (ms *MessageState) ScheduledRepeatCount() int { return ms.scheduledRepeatCount }

func (ms *MessageState) SetScheduledRepeatCount(n int) { ms.scheduledRepeatCount = n }

func (ms *MessageState) IncrScheduledRepeatCount() { ms.scheduledRepeatCount++ }

func (ms *MessageState) ResetScheduledRepeatCount() { ms.scheduledRepeatCount = 0 }

// Expired reports whether the message has expired.
func (ms *MessageState) Expired() bool { return ms.expired }

func (ms *MessageState) SetExpired(expired bool) { ms.expired = expired }

// IsExpired checks if the message TTL has elapsed.
func (ms *MessageState) IsExpired(ttl time.Duration, createdAt time.Time) bool {
	if ttl <= 0 {
		return false
	}
	return time.Since(createdAt) >= ttl
}

// ScheduledTimes returns the number of times the message has been scheduled.
func (ms *MessageState) ScheduledTimes() int { return ms.scheduledTimes }

func (ms *MessageState) SetScheduledTimes(n int) { ms.scheduledTimes = n }

func (ms *MessageState) IncrScheduledTimes() { ms.scheduledTimes++ }

// RequeueCount returns the number of times the message has been requeued.
func (ms *MessageState) RequeueCount() int { return ms.requeueCount }

func (ms *MessageState) SetRequeueCount(n int) { ms.requeueCount = n }

// ScheduledMessageParentID returns the parent scheduled message ID.
func (ms *MessageState) ScheduledMessageParentID() string { return ms.scheduledMessageParentID }

func (ms *MessageState) SetScheduledMessageParentID(id string) { ms.scheduledMessageParentID = id }

// RequeuedMessageParentID returns the parent requeued message ID.
func (ms *MessageState) RequeuedMessageParentID() string { return ms.requeuedMessageParentID }

func (ms *MessageState) SetRequeuedMessageParentID(id string) { ms.requeuedMessageParentID = id }

// Timestamp getters (return Unix milliseconds, nil if not set)

func (ms *MessageState) ScheduledAt() *int64 {
	if ms.scheduledAt == nil {
		return nil
	}
	v := ms.scheduledAt.UnixMilli()
	return &v
}

func (ms *MessageState) PublishedAt() *int64 {
	if ms.publishedAt == nil {
		return nil
	}
	v := ms.publishedAt.UnixMilli()
	return &v
}

func (ms *MessageState) RequeuedAt() *int64 {
	if ms.requeuedAt == nil {
		return nil
	}
	v := ms.requeuedAt.UnixMilli()
	return &v
}

func (ms *MessageState) ProcessingStartedAt() *int64 {
	if ms.processingStartedAt == nil {
		return nil
	}
	v := ms.processingStartedAt.UnixMilli()
	return &v
}

func (ms *MessageState) AcknowledgedAt() *int64 {
	if ms.acknowledgedAt == nil {
		return nil
	}
	v := ms.acknowledgedAt.UnixMilli()
	return &v
}

func (ms *MessageState) UnacknowledgedAt() *int64 {
	if ms.unacknowledgedAt == nil {
		return nil
	}
	v := ms.unacknowledgedAt.UnixMilli()
	return &v
}

func (ms *MessageState) DeadLetteredAt() *int64 {
	if ms.deadLetteredAt == nil {
		return nil
	}
	v := ms.deadLetteredAt.UnixMilli()
	return &v
}

func (ms *MessageState) LastRequeuedAt() *int64 {
	if ms.lastRequeuedAt == nil {
		return nil
	}
	v := ms.lastRequeuedAt.UnixMilli()
	return &v
}

func (ms *MessageState) LastUnacknowledgedAt() *int64 {
	if ms.lastUnacknowledgedAt == nil {
		return nil
	}
	v := ms.lastUnacknowledgedAt.UnixMilli()
	return &v
}

func (ms *MessageState) LastScheduledAt() *int64 {
	if ms.lastScheduledAt == nil {
		return nil
	}
	v := ms.lastScheduledAt.UnixMilli()
	return &v
}

func (ms *MessageState) LastRetriedAttemptAt() *int64 {
	if ms.lastRetriedAttemptAt == nil {
		return nil
	}
	v := ms.lastRetriedAttemptAt.UnixMilli()
	return &v
}

func (ms *MessageState) LastProcessedAt() *int64 {
	if ms.lastProcessedAt == nil {
		return nil
	}
	v := ms.lastProcessedAt.UnixMilli()
	return &v
}

// Timestamp setters

func (ms *MessageState) SetPublishedAt(ts int64)    { t := time.UnixMilli(ts); ms.publishedAt = &t }
func (ms *MessageState) SetScheduledAt(ts int64)    { t := time.UnixMilli(ts); ms.scheduledAt = &t }
func (ms *MessageState) SetRequeuedAt(ts int64)     { t := time.UnixMilli(ts); ms.requeuedAt = &t }
func (ms *MessageState) SetLastRequeuedAt(ts int64) { t := time.UnixMilli(ts); ms.lastRequeuedAt = &t }
func (ms *MessageState) SetAcknowledgedAt(ts int64) { t := time.UnixMilli(ts); ms.acknowledgedAt = &t }
func (ms *MessageState) SetUnacknowledgedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.unacknowledgedAt = &t
}
func (ms *MessageState) SetLastUnacknowledgedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastUnacknowledgedAt = &t
}
func (ms *MessageState) SetDeadLetteredAt(ts int64) { t := time.UnixMilli(ts); ms.deadLetteredAt = &t }
func (ms *MessageState) SetProcessingStartedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.processingStartedAt = &t
}
func (ms *MessageState) SetLastProcessedAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastProcessedAt = &t
}
func (ms *MessageState) SetLastScheduledAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastScheduledAt = &t
}
func (ms *MessageState) SetLastRetriedAttemptAt(ts int64) {
	t := time.UnixMilli(ts)
	ms.lastRetriedAttemptAt = &t
}

// MarkPublished records the publish timestamp.
func (ms *MessageState) MarkPublished() { now := time.Now(); ms.publishedAt = &now }

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
