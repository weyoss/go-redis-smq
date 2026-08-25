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

	internalExchange "github.com/weyoss/go-redis-smq/internal/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// DirectExchange manages direct exchanges with exact routing key matching.
type DirectExchange struct {
	store       *internalExchange.Store
	directStore *internalExchange.DirectStore
}

// NewDirectExchange creates a new direct exchange manager.
// It uses the default Redis-backed internal manager.
func NewDirectExchange() *DirectExchange {
	manager := internalExchange.NewManager()
	return &DirectExchange{
		store:       manager.Store(),
		directStore: manager.Direct(),
	}
}

// Create creates a direct exchange with the given queue policy.
func (dx *DirectExchange) Create(ctx context.Context, params *x.ExchangeParams, policy x.ExchangePolicy) error {
	if params.Type() != x.TypeDirect {
		return x.NewTypeMismatchError(x.TypeDirect, params.Type())
	}
	return dx.store.Save(ctx, params, policy)
}

// Delete removes a direct exchange and all its routing key bindings.
func (dx *DirectExchange) Delete(ctx context.Context, params *x.ExchangeParams) error {
	return dx.directStore.Delete(ctx, params)
}

// BindQueue binds a queue to a direct exchange with a specific routing key.
func (dx *DirectExchange) BindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return dx.directStore.BindQueue(ctx, queueParams, exchangeParams, routingKey)
}

// UnbindQueue removes a queue binding from a specific routing key.
func (dx *DirectExchange) UnbindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return dx.directStore.UnbindQueue(ctx, queueParams, exchangeParams, routingKey)
}

// MatchQueues returns all queues bound to a specific routing key.
func (dx *DirectExchange) MatchQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) ([]queue.QueueParams, error) {
	return dx.directStore.MatchQueues(ctx, exchangeParams, routingKey)
}

// RoutingKeys returns all routing keys registered for this direct exchange.
func (dx *DirectExchange) RoutingKeys(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]string, error) {
	return dx.directStore.RoutingKeys(ctx, exchangeParams)
}

// BoundQueues returns all queues bound to a specific routing key.
func (dx *DirectExchange) BoundQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) ([]queue.QueueParams, error) {
	return dx.directStore.BoundQueues(ctx, exchangeParams, routingKey)
}

// Bindings returns all routing key to queue mappings for this exchange.
func (dx *DirectExchange) Bindings(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) (map[string][]queue.QueueParams, error) {
	return dx.directStore.Bindings(ctx, exchangeParams)
}
