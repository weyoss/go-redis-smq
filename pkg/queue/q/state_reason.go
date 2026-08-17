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

// ── System‑only state transition reasons ─────────────────────────────

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

// ── User‑facing state transition reasons ──────────────────────────────

// StateTransitionReason contains all reasons that can be supplied by users
// through the public API when requesting a state change.
//
// The public API restricts the Reason field in StateTransitionOptions to
// this type, preventing accidental use of system‑only reasons.
type StateTransitionReason string

const (
	// ReasonManual is the default reason for manually initiated state changes.
	ReasonManual StateTransitionReason = "MANUAL"

	// ReasonScheduled indicates that the state change was scheduled.
	ReasonScheduled StateTransitionReason = "SCHEDULED"

	// ReasonEmergency indicates an emergency state change.
	ReasonEmergency StateTransitionReason = "EMERGENCY"

	// ReasonPerformance indicates a performance‑related state change.
	ReasonPerformance StateTransitionReason = "PERFORMANCE"

	// ReasonError indicates that an error triggered the state change.
	ReasonError StateTransitionReason = "ERROR"

	// ReasonConfigChange indicates that a configuration change caused the state change.
	ReasonConfigChange StateTransitionReason = "CONFIG_CHANGE"

	// ReasonTesting is used while testing state changes.
	ReasonTesting StateTransitionReason = "TESTING"

	// ReasonOther represents any uncategorised user‑provided reason.
	ReasonOther StateTransitionReason = "OTHER"
)

// ── Union type ───────────────────────────────────────────────────────

// QueueStateTransitionReason is the union of all possible state transition
// reasons – both system‑only and user‑facing.
//
// It is a distinct type (not a plain string alias) so that the compiler
// enforces explicit conversion when assigning from SystemStateTransitionReason
// or StateTransitionReason. This prevents accidental mixing and ensures the
// reason field is always one of the defined constants.
//
// Matches TypeScript `EQueueStateTransitionReason`.
type QueueStateTransitionReason string
