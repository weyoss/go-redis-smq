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

import (
	"context"
	"errors"
	"fmt"

	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Validator handles exchange validation logic.
// Encapsulates business rules for exchange type and queue policy validation.
type Validator struct {
	store      *Store
	queueStore *internalqueue.Store
}

// NewValidator creates a new exchange validator.
// It accepts the exchange store and initialises a queue store for
// queue property lookups, avoiding repeated manager creation.
func NewValidator(store *Store) *Validator {
	return &Validator{
		store:      store,
		queueStore: internalqueue.NewStore(nil),
	}
}

// ValidateQueueBinding checks whether a queue can be bound to an exchange.
//
// Validation order matches TypeScript _validateQueueBinding:
//  1. Load queue properties
//  2. Load exchange properties (may not exist yet)
//  3. Validate exchange type matches expected
//  4. Validate queue policy compatibility
//
// Returns:
//   - Props if the exchange exists and validation passes
//   - nil, nil if the exchange doesn't exist yet (new binding)
//   - Error if validation fails
func (v *Validator) ValidateQueueBinding(
	ctx context.Context,
	params *pubexchange.Params,
	queueParams *publicqueue.Params,
) (*pubexchange.Props, error) {
	// Load queue properties first (matches TypeScript order)
	queueProps, err := v.queueStore.Load(ctx, queueParams)
	if err != nil {
		return nil, fmt.Errorf("validate binding: load queue: %w", err)
	}

	// Load exchange properties
	exchangeProps, err := v.store.Load(ctx, params)
	if err != nil {
		if errors.Is(err, pubexchange.ErrNotFound) {
			// Exchange doesn't exist yet — valid for new bindings. The
			// exchange will be created when the first binding is
			// established.
			return nil, nil
		}
		return nil, fmt.Errorf("validate binding: load exchange: %w", err)
	}

	// Validate exchange type matches the expected type.
	// Prevents binding a queue to the wrong type of exchange.
	if exchangeProps.Type != params.Type() {
		return nil, pubexchange.ErrTypeMismatch
	}

	// Validate queue type satisfies the exchange's queue policy.
	if err := checkPolicy(exchangeProps, queueProps.Type); err != nil {
		return nil, err
	}

	return exchangeProps, nil
}

// PolicyForBinding validates a queue→exchange bind and returns the policy
// to persist alongside the exchange type.
//
// On the auto-create path (exchange doesn't exist yet), the policy is
// derived from the queue type — this is what makes a priority-first bind
// produce a PolicyPriority exchange instead of the zero-value
// PolicyStandard. On subsequent binds, the stored policy is validated
// against the queue and returned.
//
// Callers use the returned policy in the same HSet that writes the type,
// so the auto-create path is a single atomic write.
func (v *Validator) PolicyForBinding(
	ctx context.Context,
	params *pubexchange.Params,
	queueParams *publicqueue.Params,
) (pubexchange.Policy, error) {
	queueProps, err := v.queueStore.Load(ctx, queueParams)
	if err != nil {
		return 0, fmt.Errorf("validate binding: load queue: %w", err)
	}

	exchangeProps, err := v.store.Load(ctx, params)
	if err != nil {
		if errors.Is(err, pubexchange.ErrNotFound) {
			// Exchange doesn't exist yet — auto-create. Derive the policy
			// from the first binder's queue type.
			return policyForQueueType(queueProps.Type), nil
		}
		return 0, fmt.Errorf("validate binding: load exchange: %w", err)
	}

	if exchangeProps.Type != params.Type() {
		return 0, pubexchange.ErrTypeMismatch
	}

	if err := checkPolicy(exchangeProps, queueProps.Type); err != nil {
		return 0, err
	}

	return exchangeProps.Policy, nil
}

// policyForQueueType returns the exchange policy compatible with the
// given queue type. Used when auto-creating an exchange on first bind,
// so the auto-created exchange's policy matches the first binder's queue
// type. Mirrors the inverse of checkPolicy.
func policyForQueueType(qType publicqueue.Type) pubexchange.Policy {
	if qType == publicqueue.TypePriority {
		return pubexchange.PolicyPriority
	}
	return pubexchange.PolicyStandard
}

// checkPolicy verifies a queue type satisfies the exchange's queue policy.
//
// Policy rules match TypeScript _validateQueueBinding:
//
//	STANDARD policy: only FIFO or LIFO queues allowed
//	PRIORITY policy: only Priority queues allowed
func checkPolicy(props *pubexchange.Props, queueType publicqueue.Type) error {
	switch props.Policy {
	case pubexchange.PolicyStandard:
		// Standard exchanges require FIFO or LIFO queues.
		if queueType != publicqueue.TypeFIFO && queueType != publicqueue.TypeLIFO {
			return pubexchange.ErrPolicyViolation
		}
	case pubexchange.PolicyPriority:
		// Priority exchanges require Priority queues.
		if queueType != publicqueue.TypePriority {
			return pubexchange.ErrPolicyViolation
		}
	}
	return nil
}
