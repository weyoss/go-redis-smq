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
	"fmt"

	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Validator handles queue validation logic.
// Encapsulates business rules for queue state and configuration validation.
type Validator struct {
	store *Store
}

// NewValidator creates a new queue validator.
func NewValidator(store *Store) *Validator {
	return &Validator{store: store}
}

// Exists validates that a queue exists in Redis.
// Returns ErrNotFound if the queue doesn't exist.
func (v *Validator) Exists(ctx context.Context, queueParams *publicqueue.Params) error {
	exists, err := v.store.Exists(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate queue exists: %w", err)
	}
	if !exists {
		return publicqueue.ErrNotFound
	}
	return nil
}

// IsOperational validates that a queue exists and is in an operational state.
// Active and Paused states are considered operational.
func (v *Validator) IsOperational(ctx context.Context, queueParams *publicqueue.Params) error {
	if err := v.Exists(ctx, queueParams); err != nil {
		return err
	}

	props, err := v.store.Load(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate queue operational: %w", err)
	}

	if !props.OperationalState.IsOperational() {
		return publicqueue.ErrNotOperational
	}

	return nil
}

// CanEnqueue validates that a queue can accept new messages.
// Queue must exist and be in an operational state.
// Also checks rate limit if configured.
func (v *Validator) CanEnqueue(ctx context.Context, queueParams *publicqueue.Params) error {
	if err := v.Exists(ctx, queueParams); err != nil {
		return err
	}

	props, err := v.store.Load(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate enqueue: %w", err)
	}

	// Enqueue allowed in Active and Paused states
	if !props.OperationalState.IsOperational() {
		return publicqueue.ErrNotOperational
	}

	// Check rate limit if configured
	if props.RateLimit != nil {
		if props.MessagesCount >= int64(props.RateLimit.Limit()) {
			return publicqueue.ErrRateLimitExceeded
		}
	}

	return nil
}

// CanDequeue validates that a queue can deliver messages.
// Only Active state allows dequeue operations.
func (v *Validator) CanDequeue(ctx context.Context, queueParams *publicqueue.Params) error {
	if err := v.Exists(ctx, queueParams); err != nil {
		return err
	}

	props, err := v.store.Load(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate dequeue: %w", err)
	}

	// Dequeue only allowed in Active state
	if props.OperationalState != publicqueue.StateActive {
		return publicqueue.ErrNotOperational
	}

	return nil
}

// CanBindToExchange validates that a queue can be bound to an exchange.
// Queue must exist to be bound.
func (v *Validator) CanBindToExchange(ctx context.Context, queueParams *publicqueue.Params) error {
	return v.Exists(ctx, queueParams)
}

// CanDelete validates that a queue can be safely deleted.
// Checks that no messages are currently being processed or pending.
func (v *Validator) CanDelete(ctx context.Context, queueParams *publicqueue.Params) error {
	if err := v.Exists(ctx, queueParams); err != nil {
		return err
	}

	props, err := v.store.Load(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate queue deletion: %w", err)
	}

	if props.ProcessingMessagesCount > 0 {
		return publicqueue.ErrHasProcessingMessages
	}

	if props.PendingMessagesCount > 0 {
		return publicqueue.ErrHasPendingMessages
	}

	return nil
}

// CanPurge validates that a queue can be purged (all messages removed).
// Purge is allowed even with active consumers.
func (v *Validator) CanPurge(ctx context.Context, queueParams *publicqueue.Params) error {
	return v.Exists(ctx, queueParams)
}

// DeliveryModelMatches validates that the queue's delivery model matches
// the expected delivery model.
func (v *Validator) DeliveryModelMatches(
	ctx context.Context,
	queueParams *publicqueue.Params,
	expected publicqueue.DeliveryModel,
) error {
	if err := v.Exists(ctx, queueParams); err != nil {
		return err
	}

	props, err := v.store.Load(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("validate delivery model: %w", err)
	}

	if props.DeliveryModel != expected {
		return publicqueue.ErrDeliveryModelMismatch
	}

	return nil
}
