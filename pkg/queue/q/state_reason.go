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

// ── System‑only reasons (used internally, never exposed to the public API) ──

// SystemStateTransitionReason represents reasons that are generated
// exclusively by the system. They cannot be used in StateTransitionOptions.
type SystemStateTransitionReason string

const (
	ReasonSystemInit    SystemStateTransitionReason = "SYSTEM_INIT"
	ReasonRecovery      SystemStateTransitionReason = "RECOVERY"
	ReasonPurgeStart    SystemStateTransitionReason = "PURGE_QUEUE_START"
	ReasonPurgeCancel   SystemStateTransitionReason = "PURGE_QUEUE_CANCEL"
	ReasonPurgeFail     SystemStateTransitionReason = "PURGE_QUEUE_FAIL"
	ReasonPurgeComplete SystemStateTransitionReason = "PURGE_QUEUE_COMPLETE"
)

// ── User‑facing reasons (allowed in StateTransitionOptions) ──

// StateTransitionReason contains all reasons that are safe
// for external consumers to pass when requesting a state change.
type StateTransitionReason string

const (
	ReasonManual       StateTransitionReason = "MANUAL"
	ReasonScheduled    StateTransitionReason = "SCHEDULED"
	ReasonEmergency    StateTransitionReason = "EMERGENCY"
	ReasonPerformance  StateTransitionReason = "PERFORMANCE"
	ReasonError        StateTransitionReason = "ERROR"
	ReasonConfigChange StateTransitionReason = "CONFIG_CHANGE"
	ReasonTesting      StateTransitionReason = "TESTING"
	ReasonOther        StateTransitionReason = "OTHER"
)

// QueueStateTransitionReason is the union of all possible state transition
// reasons (system‑only + user‑facing). It is a distinct type to enforce
// compile‑time checking – raw strings cannot be assigned accidentally.
// Internal code must explicitly convert from SystemStateTransitionReason or
// StateTransitionReason when assigning to this field.
type QueueStateTransitionReason string
