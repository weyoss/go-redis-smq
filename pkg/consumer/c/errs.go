/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package c

import "errors"

var (
	// ErrNoQueues indicates the consumer has no queues registered.
	ErrNoQueues = errors.New("consumer has no queues registered")

	// ErrQueueStopped indicates the queue is stopped and cannot process messages.
	ErrQueueStopped = errors.New("queue is stopped")

	// ErrQueueLocked indicates the queue is locked and cannot process messages.
	ErrQueueLocked = errors.New("queue is locked")

	// ErrQueueInvalidState indicates the queue is in an invalid state.
	ErrQueueInvalidState = errors.New("queue is in invalid state")
)
