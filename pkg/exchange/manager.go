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

	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Manager provides common exchange operations.
// For type-specific operations, use DirectExchange, FanoutExchange, or TopicExchange.
type Manager interface {
	// Create registers a new exchange with the given params and queue policy.
	// The exchange type is determined by params.Type().
	Create(ctx context.Context, params *ExchangeParams, policy ExchangePolicy) error

	// Properties retrieves the stored configuration for an exchange.
	Properties(ctx context.Context, params *ExchangeParams) (*ExchangeProps, error)

	// Exists checks whether an exchange has been created.
	Exists(ctx context.Context, params *ExchangeParams) (bool, error)

	// ValidateType verifies an exchange exists and its type matches the expected type.
	// When required is false, a missing exchange does not cause an error.
	ValidateType(ctx context.Context, params *ExchangeParams, required bool) error

	// ValidateBinding checks whether a queue can be bound to this exchange.
	// Returns the exchange properties if the exchange exists, nil if it doesn't exist yet.
	// Validates exchange type compatibility and queue policy constraints.
	ValidateBinding(ctx context.Context, params *ExchangeParams, queueParams *queue.Params) (*ExchangeProps, error)

	// Delete removes an exchange and all its queue bindings.
	// For direct/topic exchanges, also removes routing keys and pattern bindings.
	Delete(ctx context.Context, params *ExchangeParams) error

	// ListByQueue returns all exchanges bound to a specific queue.
	ListByQueue(ctx context.Context, queueParams *queue.Params) ([]ExchangeParams, error)

	// ListAll returns every exchange across all namespaces.
	ListAll(ctx context.Context) ([]ExchangeParams, error)

	// ListByNamespace returns all exchanges within a namespace.
	ListByNamespace(ctx context.Context, namespace string) ([]ExchangeParams, error)
}
