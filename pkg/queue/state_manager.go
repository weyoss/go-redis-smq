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
)

// StateManager is the public interface for managing queue operational
// state transitions.
//
// It provides methods for pausing, resuming, stopping, locking, and
// unlocking queues, as well as retrieving the current state and transition
// history. The concrete implementation is provided by the root redissmq
// package.
type StateManager interface {
	// Current returns the latest transition, which contains the current
	// state of the queue.
	Current(ctx context.Context, params *Params) (*StateTransition, error)

	// Pause transitions a queue to the Paused state. New messages can
	// still be enqueued, but message processing stops.
	Pause(ctx context.Context, params *Params, opts *StateTransitionOptions) (*StateTransition, error)

	// Resume transitions a queue back to the Active state from Paused or
	// Stopped.
	Resume(ctx context.Context, params *Params, opts *StateTransitionOptions) (*StateTransition, error)

	// Stop transitions a queue to the Stopped state. The queue will not
	// accept or process messages.
	Stop(ctx context.Context, params *Params, opts *StateTransitionOptions) (*StateTransition, error)

	// Lock transitions a queue to the Locked state for exclusive
	// maintenance operations. The owner and lock ID are required.
	Lock(ctx context.Context, params *Params, owner LockOwner, id string, opts *StateTransitionOptions) (*StateTransition, error)

	// Unlock transitions a Locked queue back to the Active state after
	// verifying lock ownership.
	Unlock(ctx context.Context, params *Params, owner LockOwner, id string, opts *StateTransitionOptions) (*StateTransition, error)

	// History returns the full state transition history for a queue,
	// ordered from most recent to oldest.
	History(ctx context.Context, params *Params) ([]*StateTransition, error)
}
