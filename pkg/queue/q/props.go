/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package q

import "time"

// QueueProps holds the stored configuration of a queue.
type QueueProps struct {
	// Type is the queue ordering type (FIFO, LIFO, or Priority).
	Type QueueType

	// DeliveryModel is the delivery model (Point-to-Point or Pub/Sub).
	DeliveryModel DeliveryModel

	// OperationalState is the current operational state of the queue.
	OperationalState QueueState

	// MessagesCount is the total number of messages in the queue.
	MessagesCount int64

	// ScheduledMessagesCount is the number of messages scheduled for future delivery.
	ScheduledMessagesCount int64

	// PendingMessagesCount is the number of messages waiting to be consumed.
	PendingMessagesCount int64

	// ProcessingMessagesCount is the number of messages currently being processed.
	ProcessingMessagesCount int64

	// AcknowledgedMessagesCount is the number of successfully processed messages.
	AcknowledgedMessagesCount int64

	// DeadLetteredMessagesCount is the number of messages that failed permanently.
	DeadLetteredMessagesCount int64

	// DelayedMessagesCount is the number of messages waiting for a retry delay.
	DelayedMessagesCount int64

	// RequeuedMessagesCount is the number of messages waiting to be re‑inserted.
	RequeuedMessagesCount int64

	// RateLimit is the rate limit configuration, or nil if none is set.
	RateLimit *RateLimitParams

	// CreatedAt is the queue creation timestamp.
	CreatedAt time.Time

	// LastStateChangeAt is the timestamp of the last state transition.
	LastStateChangeAt time.Time

	// LockID is the lock identifier when the queue is locked.
	LockID string
}
