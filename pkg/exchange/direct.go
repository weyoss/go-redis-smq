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

// DirectExchange manages direct exchanges with exact routing‑key matching.
type DirectExchange interface {
	// Create creates a direct exchange with the given queue policy.
	// Returns TypeMismatchError if params.Type() is not TypeDirect.
	Create(ctx context.Context, params *Params, policy ExchangePolicy) error

	// Delete removes a direct exchange and all its routing key bindings.
	Delete(ctx context.Context, params *Params) error

	// BindQueue binds a queue to a direct exchange with a specific routing key.
	// The queue and exchange must be in the same namespace.
	BindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params, routingKey string) error

	// UnbindQueue removes a queue binding from a specific routing key.
	// The queue and exchange must be in the same namespace.
	UnbindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params, routingKey string) error

	// MatchQueues returns all queues bound to a specific routing key.
	MatchQueues(ctx context.Context, exchangeParams *Params, routingKey string) ([]queue.Params, error)

	// RoutingKeys returns all routing keys registered for this direct exchange.
	RoutingKeys(ctx context.Context, exchangeParams *Params) ([]string, error)

	// BoundQueues returns all queues bound to a specific routing key.
	BoundQueues(ctx context.Context, exchangeParams *Params, routingKey string) ([]queue.Params, error)

	// Bindings returns all routing key to queue mappings for this exchange.
	Bindings(ctx context.Context, exchangeParams *Params) (map[string][]queue.Params, error)
}
