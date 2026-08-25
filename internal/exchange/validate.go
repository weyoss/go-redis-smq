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
	"fmt"

	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Validator handles exchange validation logic.
// Encapsulates business rules for exchange type and queue policy validation.
type Validator struct {
	store *Store
}

// NewValidator creates a new exchange validator.
func NewValidator(store *Store) *Validator {
	return &Validator{store: store}
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
//   - ExchangeProps if the exchange exists and validation passes
//   - nil, nil if the exchange doesn't exist yet (new binding)
//   - Error if validation fails
func (v *Validator) ValidateQueueBinding(
	ctx context.Context,
	params *x.ExchangeParams,
	queueParams *publicqueue.QueueParams,
) (*x.ExchangeProps, error) {
	// Load queue properties first (matches TypeScript order)
	queueProps, err := internalqueue.NewManager().Store().Load(ctx, queueParams)
	if err != nil {
		return nil, fmt.Errorf("validate binding: load queue: %w", err)
	}

	// Load exchange properties
	exchangeProps, err := v.store.Load(ctx, params)
	if err != nil {
		if err == x.ErrNotFound {
			// Exchange doesn't exist yet - this is valid for new bindings
			// The exchange will be created when the first binding is established
			return nil, nil
		}
		return nil, fmt.Errorf("validate binding: load exchange: %w", err)
	}

	// Validate exchange type matches the expected type
	// Prevents binding a queue to the wrong type of exchange
	if exchangeProps.Type != params.Type() {
		return nil, x.NewTypeMismatchError(params.Type(), exchangeProps.Type)
	}

	// Validate queue type satisfies the exchange's queue policy
	if err := checkPolicy(exchangeProps, queueProps.Type); err != nil {
		return nil, err
	}

	return exchangeProps, nil
}

// checkPolicy verifies a queue type satisfies the exchange's queue policy.
//
// Policy rules match TypeScript _validateQueueBinding:
//
//	STANDARD policy: only FIFO or LIFO queues allowed
//	PRIORITY policy: only Priority queues allowed
func checkPolicy(props *x.ExchangeProps, queueType publicqueue.QueueType) error {
	switch props.Policy {
	case x.PolicyStandard:
		// Standard exchanges require FIFO or LIFO queues
		if queueType != publicqueue.TypeFIFO && queueType != publicqueue.TypeLIFO {
			return x.NewPolicyViolationError(
				props.Type,
				props.Policy,
				[]publicqueue.QueueType{publicqueue.TypeFIFO, publicqueue.TypeLIFO},
				queueType,
			)
		}
	case x.PolicyPriority:
		// Priority exchanges require Priority queues
		if queueType != publicqueue.TypePriority {
			return x.NewPolicyViolationError(
				props.Type,
				props.Policy,
				[]publicqueue.QueueType{publicqueue.TypePriority},
				queueType,
			)
		}
	}
	return nil
}
