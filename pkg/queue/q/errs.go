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

import "errors"

var (
	// ErrNotFound indicates that the queue does not exist.
	ErrNotFound = errors.New("queue not found")

	// ErrAlreadyExists indicates that a queue with the same name already exists.
	ErrAlreadyExists = errors.New("queue already exists")

	// ErrLocked indicates that the queue is locked and cannot be modified.
	ErrLocked = errors.New("queue is locked")

	// ErrNotOperational indicates that the queue is not in an operational state.
	ErrNotOperational = errors.New("queue is not operational")

	// ErrHasProcessingMessages indicates that the queue still has messages being processed.
	ErrHasProcessingMessages = errors.New("queue has processing messages")

	// ErrHasPendingMessages indicates that the queue still has pending messages.
	ErrHasPendingMessages = errors.New("queue has pending messages")

	// ErrRateLimitExceeded indicates that the queue rate limit has been reached.
	ErrRateLimitExceeded = errors.New("rate limit exceeded")

	// ErrDeliveryModelMismatch indicates a mismatch between the expected and actual delivery model.
	ErrDeliveryModelMismatch = errors.New("delivery model mismatch")

	// ErrQueueNotEmpty indicates that the queue still contains messages.
	ErrQueueNotEmpty = errors.New("queue is not empty")

	// ErrQueueHasActiveConsumers indicates that the queue has active consumers.
	ErrQueueHasActiveConsumers = errors.New("queue has active consumers")

	// ErrQueueHasBoundExchanges indicates that the queue is bound to an exchange.
	ErrQueueHasBoundExchanges = errors.New("queue has bound exchanges")

	// ErrConsumerSetMismatch indicates that the consumer set has changed during an operation.
	ErrConsumerSetMismatch = errors.New("consumer set mismatch")

	// ErrNameRequired indicates that the queue name is empty.
	ErrNameRequired = errors.New("queue name is required")

	// ErrInvalidName indicates that the queue name is invalid.
	ErrInvalidName = errors.New("invalid queue name")

	// ErrInvalidNamespace indicates that the namespace is invalid.
	ErrInvalidNamespace = errors.New("invalid namespace")

	// ErrInvalidRateLimit indicates that the rate limit value is invalid.
	ErrInvalidRateLimit = errors.New("invalid rate limit value")

	// ErrInvalidRateLimitInterval indicates that the rate limit interval is invalid.
	ErrInvalidRateLimitInterval = errors.New("invalid rate limit interval (min 1s)")

	// ErrInvalidTransition indicates an invalid state transition.
	ErrInvalidTransition = errors.New("invalid state transition")

	// ErrInvalidLock indicates an invalid lock identifier.
	ErrInvalidLock = errors.New("invalid lock")

	// ErrLockOwnerMismatch indicates a mismatch in lock owner.
	ErrLockOwnerMismatch = errors.New("lock owner mismatch")

	// ErrLockIDMismatch indicates a mismatch in lock ID.
	ErrLockIDMismatch = errors.New("lock ID mismatch")

	// ErrNotLocked indicates that the queue is not locked.
	ErrNotLocked = errors.New("queue is not locked")

	// ErrConsumerGroupsNotSupported indicates that consumer groups are not supported on this queue.
	ErrConsumerGroupsNotSupported = errors.New("consumer groups not supported for this queue")

	// ErrConsumerGroupNotEmpty indicates that the consumer group still contains pending messages.
	ErrConsumerGroupNotEmpty = errors.New("consumer group is not empty")

	// ErrConsumerGroupHasActiveConsumers indicates that the consumer group has active consumers.
	ErrConsumerGroupHasActiveConsumers = errors.New("consumer group has active consumers")

	// ErrAuditDisabled indicates that message audit is disabled.
	ErrAuditDisabled = errors.New("message audit is disabled for this operation")
)
