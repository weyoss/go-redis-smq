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
type StateTransition struct {
	From        *QueueState                `json:"from"`
	To          QueueState                 `json:"to"`
	Reason      QueueStateTransitionReason `json:"reason"`
	Timestamp   int64                      `json:"timestamp"`
	Description string                     `json:"description,omitempty"`
	LockID      *string                    `json:"lockId,omitempty"`
	Owner       *LockOwner                 `json:"lockOwner,omitempty"`
	Metadata    map[string]interface{}     `json:"metadata,omitempty"`
}

// StateTransitionOptions carries optional parameters for state change requests.
// Reason accepts only user‑facing reasons to prevent accidental use of system constants.
type StateTransitionOptions struct {
	Reason      *StateTransitionReason `json:"reason,omitempty"` // user‑facing only
	Description *string                `json:"description,omitempty"`
	LockID      *string                `json:"lockId,omitempty"`
	Owner       *LockOwner             `json:"lockOwner,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}
