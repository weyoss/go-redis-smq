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

// StateTransition records a single state change event.
//
// The JSON representation matches the TypeScript IQueueStateTransition
// interface to ensure cross‑language compatibility.
type StateTransition struct {
	// From is the previous state. It is nil for the initial transition.
	From *QueueState `json:"from"`

	// To is the new state.
	To QueueState `json:"to"`

	// Reason explains why the transition occurred.
	// It must be one of the values defined by
	// SystemStateTransitionReason or StateTransitionReason.
	Reason QueueStateTransitionReason `json:"reason"`

	// Timestamp is the Unix timestamp in milliseconds when the transition
	// took place.
	Timestamp int64 `json:"timestamp"`

	// Description is an optional human‑readable explanation of the transition.
	Description string `json:"description,omitempty"`

	// LockID is the lock identifier, present only for LOCKED ↔ ACTIVE
	// transitions.
	LockID *string `json:"lockId,omitempty"`

	// Owner is the lock owner, present only for LOCKED ↔ ACTIVE transitions.
	Owner *LockOwner `json:"lockOwner,omitempty"`

	// Metadata contains optional additional context as key‑value pairs.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// StateTransitionOptions carries optional parameters for state change
// requests. The zero value is valid and indicates that default values
// should be used.
//
// The Reason field accepts only user‑facing reasons
// (StateTransitionReason) to prevent system‑only reasons from being
// passed through the public API.
type StateTransitionOptions struct {
	// Reason is the user‑facing reason for the transition.
	// If nil, the default reason ReasonManual is used.
	Reason *StateTransitionReason `json:"reason,omitempty"`

	// Description is an optional human‑readable description of the
	// transition.
	Description *string `json:"description,omitempty"`

	// LockID is the lock identifier, required only for LOCKED ↔ ACTIVE
	// transitions.
	LockID *string `json:"lockId,omitempty"`

	// Owner is the lock owner, required only for LOCKED ↔ ACTIVE
	// transitions.
	Owner *LockOwner `json:"lockOwner,omitempty"`

	// Metadata contains arbitrary key‑value pairs to attach to the
	// transition.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
