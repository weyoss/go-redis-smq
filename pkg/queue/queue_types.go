/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

// QueueType represents the ordering semantics of a queue.
// Values match TypeScript EQueueType enum for cross-language compatibility.
// Integer values are persisted in Redis and must not be changed.
type QueueType int

const (
	// TypeLIFO delivers messages in last-in-first-out order.
	TypeLIFO QueueType = iota // 0

	// TypeFIFO delivers messages in first-in-first-out order.
	TypeFIFO // 1

	// TypePriority delivers messages based on priority level.
	TypePriority // 2
)

// Int returns the integer representation for Redis storage.
func (t QueueType) Int() int { return int(t) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (t QueueType) String() string {
	switch t {
	case TypeFIFO:
		return "fifo"
	case TypeLIFO:
		return "lifo"
	case TypePriority:
		return "priority"
	default:
		return "unknown"
	}
}

// IsValid reports whether the queue type value is within the valid range.
// Valid types are TypeLIFO through TypePriority.
func (t QueueType) IsValid() bool {
	return t >= TypeLIFO && t <= TypePriority
}

// DeliveryModel defines how messages are delivered to consumers.
// Values match TypeScript DeliveryModel enum for cross-language compatibility.
// Integer values are persisted in Redis and must not be changed.
type DeliveryModel int

const (
	// DeliveryPointToPoint delivers each message to exactly one consumer.
	// Best for task queues and workload distribution.
	DeliveryPointToPoint DeliveryModel = iota // 0

	// DeliveryPubSub delivers each message to all consumers in a group.
	// Best for event broadcasting and fan-out patterns.
	DeliveryPubSub // 1
)

// Int returns the integer representation for Redis storage.
func (dm DeliveryModel) Int() int { return int(dm) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (dm DeliveryModel) String() string {
	switch dm {
	case DeliveryPointToPoint:
		return "point_to_point"
	case DeliveryPubSub:
		return "pub_sub"
	default:
		return "unknown"
	}
}

// IsValid reports whether the delivery model value is within the valid range.
func (dm DeliveryModel) IsValid() bool {
	return dm >= DeliveryPointToPoint && dm <= DeliveryPubSub
}

// QueueState represents a queue's operational state.
// Integer values are persisted in Redis and must not be changed.
type QueueState int

const (
	// StateActive indicates the queue processes messages normally.
	StateActive QueueState = iota // 0

	// StatePaused indicates the queue stops processing but accepts new messages.
	StatePaused // 1

	// StateStopped indicates the queue does not accept or process messages.
	StateStopped // 2

	// StateLocked indicates the queue is locked for exclusive operations.
	StateLocked // 3
)

// Int returns the integer representation for Redis storage.
func (s QueueState) Int() int { return int(s) }

// String returns a human-readable representation.
// Unknown values return "unknown".
func (s QueueState) String() string {
	switch s {
	case StateActive:
		return "active"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	case StateLocked:
		return "locked"
	default:
		return "unknown"
	}
}

// IsOperational returns true if the queue can process messages.
// Active and Paused states are considered operational.
func (s QueueState) IsOperational() bool {
	return s == StateActive || s == StatePaused
}

// IsValid reports whether the state value is within the valid range.
// Valid states are StateActive through StateLocked.
func (s QueueState) IsValid() bool {
	return s >= StateActive && s <= StateLocked
}

// allowedTransitions defines the valid state transition graph.
// Use CanTransitionTo() to query — do not access this map directly.
var allowedTransitions = map[QueueState][]QueueState{
	StateActive:  {StatePaused, StateLocked, StateStopped},
	StatePaused:  {StateActive, StateStopped, StateLocked},
	StateStopped: {StateActive},
	StateLocked:  {StateActive, StateStopped},
}

// CanTransitionTo reports whether transitioning from s to target is allowed.
// It returns false for invalid or unknown states.
func (s QueueState) CanTransitionTo(target QueueState) bool {
	for _, allowed := range allowedTransitions[s] {
		if allowed == target {
			return true
		}
	}
	return false
}

// LockOwner identifies the entity holding a queue lock.
//
// The integer value is persisted in Redis and must match the TypeScript
// EQueueStateLockOwner enum for cross-language compatibility.
type LockOwner int

const (
	// LockOwnerPurgeJob indicates a queue purge operation holds the lock.
	LockOwnerPurgeJob LockOwner = iota // 0
)

// String returns a human-readable representation.
// Unknown values return "UNKNOWN".
func (o LockOwner) String() string {
	switch o {
	case LockOwnerPurgeJob:
		return "PURGE_JOB"
	default:
		return "UNKNOWN"
	}
}

// Int returns the integer representation for Redis storage.
func (o LockOwner) Int() int { return int(o) }
