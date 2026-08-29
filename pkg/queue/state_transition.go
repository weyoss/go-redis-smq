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

// SystemStateTransitionReason represents reasons that are generated
// exclusively by the system and must never be set through the public API.
//
// These reasons are typically produced during internal operations such as
// queue creation, recovery, and purge jobs.
type SystemStateTransitionReason string

const (
	// ReasonSystemInit is the reason for a queue's initial state transition
	// when it is created.
	ReasonSystemInit SystemStateTransitionReason = "SYSTEM_INIT"

	// ReasonRecovery is used when a queue state is recovered after a failure.
	ReasonRecovery SystemStateTransitionReason = "RECOVERY"

	// ReasonPurgeStart is used when a purge operation starts and locks a queue.
	ReasonPurgeStart SystemStateTransitionReason = "PURGE_QUEUE_START"

	// ReasonPurgeCancel is used when a purge operation is cancelled.
	ReasonPurgeCancel SystemStateTransitionReason = "PURGE_QUEUE_CANCEL"

	// ReasonPurgeFail is used when a purge operation fails.
	ReasonPurgeFail SystemStateTransitionReason = "PURGE_QUEUE_FAIL"

	// ReasonPurgeComplete is used when a purge operation completes successfully.
	ReasonPurgeComplete SystemStateTransitionReason = "PURGE_QUEUE_COMPLETE"
)

// StateTransitionReason contains all reasons that can be supplied by users
// through the public API when requesting a state change.
//
// The public API restricts the Reason field in StateTransitionOptions to
// this type, preventing accidental use of system-only reasons.
type StateTransitionReason string

const (
	// ReasonManual is the default reason for manually initiated state changes.
	ReasonManual StateTransitionReason = "MANUAL"

	// ReasonScheduled indicates that the state change was scheduled.
	ReasonScheduled StateTransitionReason = "SCHEDULED"

	// ReasonEmergency indicates an emergency state change.
	ReasonEmergency StateTransitionReason = "EMERGENCY"

	// ReasonPerformance indicates a performance-related state change.
	ReasonPerformance StateTransitionReason = "PERFORMANCE"

	// ReasonError indicates that an error triggered the state change.
	ReasonError StateTransitionReason = "ERROR"

	// ReasonConfigChange indicates that a configuration change caused the state change.
	ReasonConfigChange StateTransitionReason = "CONFIG_CHANGE"

	// ReasonTesting is used while testing state changes.
	ReasonTesting StateTransitionReason = "TESTING"

	// ReasonOther represents any uncategorised user-provided reason.
	ReasonOther StateTransitionReason = "OTHER"
)

// TransitionReason is the union of all possible state transition
// reasons – both system-only and user-facing.
//
// It is a distinct type (not a plain string alias) so that the compiler
// enforces explicit conversion when assigning from SystemStateTransitionReason
// or StateTransitionReason. This prevents accidental mixing and ensures the
// reason field is always one of the defined constants.
//
// Matches TypeScript `EQueueStateTransitionReason`.
type TransitionReason string

// StateTransition records a single state change event.
//
// The JSON representation matches the TypeScript IQueueStateTransition
// interface to ensure cross-language compatibility.
type StateTransition struct {
	// From is the previous state. It is nil for the initial transition.
	From *State `json:"from"`

	// To is the new state.
	To State `json:"to"`

	// Reason explains why the transition occurred.
	Reason TransitionReason `json:"reason"`

	// Timestamp is the Unix timestamp in milliseconds when the transition
	// took place.
	Timestamp int64 `json:"timestamp"`

	// Description is an optional human-readable explanation of the transition.
	Description string `json:"description,omitempty"`

	// LockID is the lock identifier, present only for LOCKED ↔ ACTIVE
	// transitions.
	LockID *string `json:"lockId,omitempty"`

	// Owner is the lock owner, present only for LOCKED ↔ ACTIVE transitions.
	Owner *LockOwner `json:"lockOwner,omitempty"`

	// Metadata contains optional additional context as key-value pairs.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// StateTransitionOptions carries optional parameters for state change
// requests. The zero value is valid and indicates that default values
// should be used.
//
// The Reason field accepts only user-facing reasons
// (StateTransitionReason) to prevent system-only reasons from being
// passed through the public API.
type StateTransitionOptions struct {
	// Reason is the user-facing reason for the transition.
	// If nil, the default reason ReasonManual is used.
	Reason *StateTransitionReason `json:"reason,omitempty"`

	// Description is an optional human-readable description of the transition.
	Description *string `json:"description,omitempty"`

	// LockID is the lock identifier, required only for LOCKED ↔ ACTIVE
	// transitions.
	LockID *string `json:"lockId,omitempty"`

	// Owner is the lock owner, required only for LOCKED ↔ ACTIVE
	// transitions.
	Owner *LockOwner `json:"lockOwner,omitempty"`

	// Metadata contains arbitrary key-value pairs to attach to the
	// transition.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
