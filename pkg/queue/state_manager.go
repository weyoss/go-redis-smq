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

import (
	"context"

	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// StateManager controls queue operational state transitions.
// All methods delegate to the internal RDB layer which executes Lua scripts atomically.
//
// Common flows:
//   - Pause/Resume: Temporarily stop/start message processing
//   - Stop: Halt all message handling
//   - Lock/Unlock: Exclusive access for maintenance operations (e.g., purge)
type StateManager struct {
	state *internalQueue.State
}

// NewStateManager creates a queue state manager.
func NewStateManager() *StateManager {
	return &StateManager{
		state: internalQueue.NewState(),
	}
}

// Current returns the latest transition (which contains the current state) for a queue.
func (sm *StateManager) Current(ctx context.Context, params *q.QueueParams) (*q.StateTransition, error) {
	return sm.state.FetchCurrent(ctx, params)
}

// Pause transitions a queue to Paused, stopping message processing.
// New messages can still be enqueued while paused.
func (sm *StateManager) Pause(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, q.StatePaused, opts)
}

// Resume transitions a queue back to Active from Paused or Stopped.
func (sm *StateManager) Resume(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, q.StateActive, opts)
}

// Stop transitions a queue to Stopped, halting all message handling.
// Stopped queues cannot accept or process messages.
func (sm *StateManager) Stop(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, q.StateStopped, opts)
}

// Lock transitions a queue to Locked for exclusive operations.
// Requires an owner and lock ID for identification.
// Only the lock holder can unlock the queue.
func (sm *StateManager) Lock(ctx context.Context, params *q.QueueParams, owner q.LockOwner, id string, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return sm.state.AcquireLock(ctx, params, owner, id, opts)
}

// Unlock transitions a Locked queue back to Active after verifying lock ownership.
// The owner and lock ID must match the current lock holder.
func (sm *StateManager) Unlock(ctx context.Context, params *q.QueueParams, owner q.LockOwner, id string, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return sm.state.ReleaseLock(ctx, params, owner, id, opts)
}

// History returns the full state transition history for a queue.
// Ordered from most recent to oldest.
func (sm *StateManager) History(ctx context.Context, params *q.QueueParams) ([]*q.StateTransition, error) {
	return sm.state.FetchHistory(ctx, params)
}

// Default state manager instance used by package-level convenience functions.
var defaultStateManager = NewStateManager()

// Current returns the current state using the default state manager.
func Current(ctx context.Context, params *q.QueueParams) (*q.StateTransition, error) {
	return defaultStateManager.Current(ctx, params)
}

// Pause transitions to Paused using the default state manager.
func Pause(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return defaultStateManager.Pause(ctx, params, opts)
}

// Resume transitions to Active using the default state manager.
func Resume(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return defaultStateManager.Resume(ctx, params, opts)
}

// Stop transitions to Stopped using the default state manager.
func Stop(ctx context.Context, params *q.QueueParams, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return defaultStateManager.Stop(ctx, params, opts)
}

// Lock transitions to Locked using the default state manager.
func Lock(ctx context.Context, params *q.QueueParams, owner q.LockOwner, id string, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return defaultStateManager.Lock(ctx, params, owner, id, opts)
}

// Unlock transitions to Active using the default state manager.
func Unlock(ctx context.Context, params *q.QueueParams, owner q.LockOwner, id string, opts *q.StateTransitionOptions) (*q.StateTransition, error) {
	return defaultStateManager.Unlock(ctx, params, owner, id, opts)
}

// History returns state history using the default state manager.
func History(ctx context.Context, params *q.QueueParams) ([]*q.StateTransition, error) {
	return defaultStateManager.History(ctx, params)
}
