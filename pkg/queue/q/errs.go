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
	// Queue operation errors
	ErrNotFound                = errors.New("queue not found")
	ErrAlreadyExists           = errors.New("queue already exists")
	ErrLocked                  = errors.New("queue is locked")
	ErrNotOperational          = errors.New("queue is not operational")
	ErrHasProcessingMessages   = errors.New("queue has processing messages")
	ErrHasPendingMessages      = errors.New("queue has pending messages")
	ErrRateLimitExceeded       = errors.New("rate limit exceeded")
	ErrDeliveryModelMismatch   = errors.New("delivery model mismatch")
	ErrQueueNotEmpty           = errors.New("queue is not empty")
	ErrQueueHasActiveConsumers = errors.New("queue has active consumers")
	ErrQueueHasBoundExchanges  = errors.New("queue has bound exchanges")
	ErrConsumerSetMismatch     = errors.New("consumer set mismatch")

	// Param validation errors
	ErrNameRequired     = errors.New("queue name is required")
	ErrInvalidName      = errors.New("invalid queue name")
	ErrInvalidNamespace = errors.New("invalid namespace")

	// Rate limit errors
	ErrInvalidRateLimit         = errors.New("invalid rate limit value")
	ErrInvalidRateLimitInterval = errors.New("invalid rate limit interval (min 1s)")

	// State transition errors
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrInvalidLock       = errors.New("invalid lock")
	ErrLockOwnerMismatch = errors.New("lock owner mismatch")
	ErrLockIDMismatch    = errors.New("lock ID mismatch")
	ErrNotLocked         = errors.New("queue is not locked")

	// Consumer group errors
	ErrConsumerGroupsNotSupported      = errors.New("consumer groups not supported for this queue")
	ErrConsumerGroupNotEmpty           = errors.New("consumer group is not empty")
	ErrConsumerGroupHasActiveConsumers = errors.New("consumer group has active consumers")

	// Audit errors
	ErrAuditDisabled = errors.New("message audit is disabled for this operation")
)
