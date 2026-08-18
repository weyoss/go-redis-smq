/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package msg defines message types, statuses, priorities, and related
// configuration used by the public message API.
package msg

// MessageStatus represents the current lifecycle state of a message.
// Integer values are persisted in Redis and must not be changed.
// Matches TypeScript EMessagePropertyStatus enum.
type MessageStatus int

const (
	// StatusNew indicates that a message has been created but not yet
	// published.
	StatusNew MessageStatus = iota // 0

	// StatusPending indicates that a message is waiting to be consumed.
	StatusPending // 1

	// StatusProcessing indicates that a message is currently being handled
	// by a consumer.
	StatusProcessing // 2

	// StatusScheduled indicates that a message is scheduled for future
	// delivery.
	StatusScheduled // 3

	// StatusAcknowledged indicates that a message has been successfully
	// processed and acknowledged.
	StatusAcknowledged // 4

	// StatusUnackRequeuing indicates that a message failed and is waiting
	// to be requeued immediately.
	StatusUnackRequeuing // 5

	// StatusUnackDelaying indicates that a message failed and is waiting
	// for a retry delay before being requeued.
	StatusUnackDelaying // 6

	// StatusDeadLettered indicates that a message has failed permanently
	// and was moved to the dead-letter queue.
	StatusDeadLettered // 7
)

// Int returns the integer representation for Redis storage.
func (s MessageStatus) Int() int { return int(s) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (s MessageStatus) String() string {
	switch s {
	case StatusNew:
		return "new"
	case StatusPending:
		return "pending"
	case StatusProcessing:
		return "processing"
	case StatusScheduled:
		return "scheduled"
	case StatusAcknowledged:
		return "acknowledged"
	case StatusUnackRequeuing:
		return "unack_requeuing"
	case StatusUnackDelaying:
		return "unack_delaying"
	case StatusDeadLettered:
		return "dead_lettered"
	default:
		return "unknown"
	}
}

// IsTerminal reports whether the message has reached a final state
// (Acknowledged or DeadLettered).
func (s MessageStatus) IsTerminal() bool {
	return s == StatusAcknowledged || s == StatusDeadLettered
}

// IsPending reports whether the message is waiting to be consumed.
func (s MessageStatus) IsPending() bool { return s == StatusPending }

// IsProcessing reports whether the message is currently being processed.
func (s MessageStatus) IsProcessing() bool { return s == StatusProcessing }

// IsRequeuable reports whether the message can be requeued.
// Only acknowledged and dead-lettered messages are requeuable.
func (s MessageStatus) IsRequeuable() bool {
	return s == StatusAcknowledged || s == StatusDeadLettered
}

// IsValid reports whether the status value is within the valid range.
func (s MessageStatus) IsValid() bool {
	return s >= StatusNew && s <= StatusDeadLettered
}
