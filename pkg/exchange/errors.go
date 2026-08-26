/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

import "errors"

// Sentinel errors for exchange operations.
var (
	// ErrNotFound indicates the exchange does not exist.
	ErrNotFound = errors.New("exchange not found")

	// ErrAlreadyExists indicates an exchange with the same name already exists.
	ErrAlreadyExists = errors.New("exchange already exists")

	// ErrNamespaceMismatch indicates the queue and exchange are in different namespaces.
	ErrNamespaceMismatch = errors.New("namespace mismatch")

	// ErrQueueAlreadyBound indicates the queue is already bound to this exchange/routing key.
	ErrQueueAlreadyBound = errors.New("queue already bound to exchange")

	// ErrQueueNotBound indicates the queue is not bound to this exchange/routing key.
	ErrQueueNotBound = errors.New("queue not bound to exchange")

	// ErrHasBoundQueues indicates the exchange cannot be deleted because queues are still bound.
	ErrHasBoundQueues = errors.New("exchange has bound queues")

	// ErrInvalidPattern indicates the topic binding pattern is not valid.
	ErrInvalidPattern = errors.New("invalid topic binding pattern")

	// ErrInvalidRoutingKey indicates the routing key is not valid.
	ErrInvalidRoutingKey = errors.New("invalid routing key")

	// ErrTypeMismatch indicates the stored exchange type doesn't match the expected type.
	ErrTypeMismatch = errors.New("exchange type mismatch")

	// ErrPolicyViolation indicates a queue doesn't satisfy the exchange's queue policy.
	ErrPolicyViolation = errors.New("queue policy violation")

	// Parameter validation errors

	// ErrNameRequired indicates the exchange name was empty.
	ErrNameRequired = errors.New("exchange name is required")

	// ErrInvalidName indicates the exchange name failed validation.
	ErrInvalidName = errors.New("invalid exchange name")

	// ErrInvalidNamespace indicates the namespace failed validation.
	ErrInvalidNamespace = errors.New("invalid namespace")
)
