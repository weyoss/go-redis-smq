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

// StateTransferable holds the message lifecycle state in a form suitable
// for JSON serialization between RedisSMQ components and across language
// implementations.
//
// It matches the TypeScript IMessageStateTransferable format.
type StateTransferable struct {
	// UUID is the unique identifier of the message state instance.
	UUID string `json:"uuid"`

	// ScheduledAt is the Unix timestamp in milliseconds when the message
	// was scheduled for future delivery. Nil if not scheduled.
	ScheduledAt *int64 `json:"scheduledAt,omitempty"`

	// PublishedAt is the Unix timestamp in milliseconds when the message
	// was first published.
	PublishedAt *int64 `json:"publishedAt,omitempty"`

	// RequeuedAt is the Unix timestamp in milliseconds when the message
	// was first requeued.
	RequeuedAt *int64 `json:"requeuedAt,omitempty"`

	// ProcessingStartedAt is the Unix timestamp in milliseconds when the
	// most recent processing attempt started.
	ProcessingStartedAt *int64 `json:"processingStartedAt,omitempty"`

	// AcknowledgedAt is the Unix timestamp in milliseconds when the message
	// was acknowledged.
	AcknowledgedAt *int64 `json:"acknowledgedAt,omitempty"`

	// UnacknowledgedAt is the Unix timestamp in milliseconds of the most
	// recent unacknowledgment.
	UnacknowledgedAt *int64 `json:"unacknowledgedAt,omitempty"`

	// DeadLetteredAt is the Unix timestamp in milliseconds when the message
	// was dead-lettered.
	DeadLetteredAt *int64 `json:"deadLetteredAt,omitempty"`

	// LastRequeuedAt is the Unix timestamp in milliseconds of the most
	// recent requeue.
	LastRequeuedAt *int64 `json:"lastRequeuedAt,omitempty"`

	// LastUnacknowledgedAt is the Unix timestamp in milliseconds of the
	// previous unacknowledgment.
	LastUnacknowledgedAt *int64 `json:"lastUnacknowledgedAt,omitempty"`

	// LastScheduledAt is the Unix timestamp in milliseconds when the message
	// was last scheduled.
	LastScheduledAt *int64 `json:"lastScheduledAt,omitempty"`

	// LastRetriedAttemptAt is the Unix timestamp in milliseconds of the last
	// retry attempt.
	LastRetriedAttemptAt *int64 `json:"lastRetriedAttemptAt,omitempty"`

	// LastProcessedAt is the Unix timestamp in milliseconds when the message
	// was last processed.
	LastProcessedAt *int64 `json:"lastProcessedAt,omitempty"`

	// ScheduledCronFired indicates whether the CRON trigger has fired for
	// this message.
	ScheduledCronFired bool `json:"scheduledCronFired"`

	// Attempts is the number of consumption attempts so far.
	Attempts int `json:"attempts"`

	// ScheduledRepeatCount is the number of repeat deliveries that have
	// occurred.
	ScheduledRepeatCount int `json:"scheduledRepeatCount"`

	// RequeueCount is the number of times the message has been requeued.
	RequeueCount int `json:"requeueCount"`

	// Expired indicates whether the message has expired due to its TTL.
	Expired bool `json:"expired"`

	// EffectiveScheduledDelay is the current effective scheduled delay in
	// milliseconds.
	EffectiveScheduledDelay int64 `json:"effectiveScheduledDelay"`

	// ScheduledTimes is the number of times the message has been scheduled.
	ScheduledTimes int `json:"scheduledTimes"`

	// ScheduledMessageParentID is the ID of the parent scheduled message,
	// if this message was created as part of a repeat cycle.
	ScheduledMessageParentID string `json:"scheduledMessageParentId,omitempty"`

	// RequeuedMessageParentID is the ID of the original message that was
	// requeued, if this message is a requeue copy.
	RequeuedMessageParentID string `json:"requeuedMessageParentId,omitempty"`
}
