/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package c defines configuration-related types and sentinel errors for
// RedisSMQ consumers.
package c

import "errors"

// Common consumer errors.
var (
	// ErrNoQueues indicates that the consumer has no queues registered.
	// It is returned by Consumer.Run when no handlers were added.
	ErrNoQueues = errors.New("consumer has no queues registered")

	// ErrQueueStopped indicates that the queue is stopped and cannot
	// process messages.
	ErrQueueStopped = errors.New("queue is stopped")

	// ErrQueueLocked indicates that the queue is locked and cannot process
	// messages.
	ErrQueueLocked = errors.New("queue is locked")

	// ErrQueueInvalidState indicates that the queue is in an invalid
	// operational state for consumption.
	ErrQueueInvalidState = errors.New("queue is in invalid state")
)
