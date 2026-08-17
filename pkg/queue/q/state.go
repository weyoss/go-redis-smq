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
