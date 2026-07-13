/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package p

import "errors"

var (
	ErrNotRunning            = errors.New("producer is not running")
	ErrExchangeRequired      = errors.New("message must have a queue or exchange")
	ErrRoutingKeyRequired    = errors.New("routing key required for direct/topic exchange")
	ErrNoMatchingQueues      = errors.New("no matching queues found for exchange")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrPriorityRequired      = errors.New("message priority required")
	ErrMessageAlreadyExists  = errors.New("message already exists")
	ErrPriorityNotEnabled    = errors.New("priority queuing not enabled")
	ErrUnknownQueueType      = errors.New("unknown queue type")
	ErrInvalidQueueState     = errors.New("queue is in invalid state")
)
