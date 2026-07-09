/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package x

import (
	"errors"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

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
)

// TypeMismatchError indicates the stored exchange type doesn't match the expected type.
// This can occur when trying to use a direct exchange operation on a fanout exchange.
type TypeMismatchError struct {
	Expected ExchangeType
	Actual   ExchangeType
}

// NewTypeMismatchError creates a new TypeMismatchError.
func NewTypeMismatchError(expected, actual ExchangeType) *TypeMismatchError {
	return &TypeMismatchError{
		Expected: expected,
		Actual:   actual,
	}
}

// Error returns a formatted error message.
func (e *TypeMismatchError) Error() string {
	return fmt.Sprintf("exchange type mismatch: expected %s, got %s", e.Expected, e.Actual)
}

// PolicyViolationError indicates a queue doesn't satisfy the exchange's queue policy.
// This occurs when trying to bind a priority queue to a standard exchange, or vice versa.
type PolicyViolationError struct {
	ExchangeType ExchangeType
	Policy       ExchangePolicy
	AllowedKinds []q.QueueType
	ActualKind   q.QueueType
}

// NewPolicyViolationError creates a new PolicyViolationError.
func NewPolicyViolationError(
	exchangeType ExchangeType,
	policy ExchangePolicy,
	allowed []q.QueueType,
	actual q.QueueType,
) *PolicyViolationError {
	return &PolicyViolationError{
		ExchangeType: exchangeType,
		Policy:       policy,
		AllowedKinds: allowed,
		ActualKind:   actual,
	}
}

// Error returns a formatted error message describing the policy violation.
func (e *PolicyViolationError) Error() string {
	return fmt.Sprintf(
		"queue policy violation for %s exchange: %s policy allows %v queues, got %s",
		e.ExchangeType, e.Policy, e.AllowedKinds, e.ActualKind,
	)
}
