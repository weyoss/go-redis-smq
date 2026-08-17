/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package p provides sentinel errors returned by the RedisSMQ producer.
//
// These errors describe common failure conditions that can occur while
// publishing messages. Use errors.Is to check for a specific cause.
package p

import "errors"

// Common producer errors.
var (
	// ErrNotRunning indicates that the producer has not been started with Run.
	ErrNotRunning = errors.New("producer is not running")

	// ErrExchangeRequired indicates that the message has neither a queue nor
	// an exchange destination set.
	ErrExchangeRequired = errors.New("message must have a queue or exchange")

	// ErrRoutingKeyRequired indicates that a routing key is required for the
	// chosen exchange type (direct or topic).
	ErrRoutingKeyRequired = errors.New("routing key required for direct/topic exchange")

	// ErrNoMatchingQueues indicates that no queues are bound to the exchange
	// for the given routing key or pattern.
	ErrNoMatchingQueues = errors.New("no matching queues found for exchange")

	// ErrConsumerGroupNotFound indicates that a message was published to a
	// non-existent consumer group.
	ErrConsumerGroupNotFound = errors.New("consumer group not found")

	// ErrPriorityRequired indicates that a priority queue requires a message
	// priority before publishing.
	ErrPriorityRequired = errors.New("message priority required")

	// ErrMessageAlreadyExists indicates that a message with the same ID
	// already exists.
	ErrMessageAlreadyExists = errors.New("message already exists")

	// ErrPriorityNotEnabled indicates that a priority was set on a message
	// but the target queue does not support priorities.
	ErrPriorityNotEnabled = errors.New("priority queuing not enabled")

	// ErrUnknownQueueType indicates that the target queue has an unrecognised
	// queue type.
	ErrUnknownQueueType = errors.New("unknown queue type")

	// ErrInvalidQueueState indicates that the target queue is in an invalid
	// operational state for publishing.
	ErrInvalidQueueState = errors.New("queue is in invalid state")
)
