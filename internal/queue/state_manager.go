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

	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// StateManager is the concrete implementation of the public queue state
// manager interface. It delegates to the internal state transition logic.
type StateManager struct {
	state *State
}

// NewStateManager creates a new concrete state manager that satisfies the
// public queue.StateManager interface.
func NewStateManager() publicqueue.StateManager {
	return &StateManager{state: NewState()}
}

func (sm *StateManager) Current(ctx context.Context, params *publicqueue.QueueParams) (*publicqueue.StateTransition, error) {
	return sm.state.FetchCurrent(ctx, params)
}

func (sm *StateManager) Pause(ctx context.Context, params *publicqueue.QueueParams, opts *publicqueue.StateTransitionOptions) (*publicqueue.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, publicqueue.StatePaused, opts)
}

func (sm *StateManager) Resume(ctx context.Context, params *publicqueue.QueueParams, opts *publicqueue.StateTransitionOptions) (*publicqueue.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, publicqueue.StateActive, opts)
}

func (sm *StateManager) Stop(ctx context.Context, params *publicqueue.QueueParams, opts *publicqueue.StateTransitionOptions) (*publicqueue.StateTransition, error) {
	return sm.state.TransitionTo(ctx, params, publicqueue.StateStopped, opts)
}

func (sm *StateManager) Lock(ctx context.Context, params *publicqueue.QueueParams, owner publicqueue.LockOwner, id string, opts *publicqueue.StateTransitionOptions) (*publicqueue.StateTransition, error) {
	return sm.state.AcquireLock(ctx, params, owner, id, opts)
}

func (sm *StateManager) Unlock(ctx context.Context, params *publicqueue.QueueParams, owner publicqueue.LockOwner, id string, opts *publicqueue.StateTransitionOptions) (*publicqueue.StateTransition, error) {
	return sm.state.ReleaseLock(ctx, params, owner, id, opts)
}

func (sm *StateManager) History(ctx context.Context, params *publicqueue.QueueParams) ([]*publicqueue.StateTransition, error) {
	return sm.state.FetchHistory(ctx, params)
}
