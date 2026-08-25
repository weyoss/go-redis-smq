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

// FanoutExchange manages fanout exchanges that broadcast to all bound queues.
//
// Fanout exchanges route messages to every bound queue, ignoring routing keys.
// Ideal for pub/sub patterns where every consumer should receive the message.
//
// Example:
//
//	fx := exchange.NewFanoutExchange()
//	fx.Create(ctx, params, x.PolicyStandard)
//	fx.BindQueue(ctx, queueParams, exchangeParams)
//	queues, _ := fx.MatchQueues(ctx, exchangeParams)
type FanoutExchange struct {
	store       *internalExchange.Store
	fanoutStore *internalExchange.FanoutStore
}

// NewFanoutExchange creates a new fanout exchange manager.
// It uses the default Redis-backed internal manager.
func NewFanoutExchange() *FanoutExchange {
	manager := internalExchange.NewManager()
	return &FanoutExchange{
		store:       manager.Store(),
		fanoutStore: manager.Fanout(),
	}
}

// Create creates a fanout exchange with the given queue policy.
// Returns TypeMismatchError if params.Type() is not TypeFanout.
func (fx *FanoutExchange) Create(ctx context.Context, params *x.ExchangeParams, policy x.ExchangePolicy) error {
	if params.Type() != x.TypeFanout {
		return x.NewTypeMismatchError(x.TypeFanout, params.Type())
	}
	return fx.store.Save(ctx, params, policy)
}

// Delete removes a fanout exchange and all its queue bindings.
// Returns ErrHasBoundQueues if any queues are still bound.
func (fx *FanoutExchange) Delete(ctx context.Context, params *x.ExchangeParams) error {
	return fx.fanoutStore.Delete(ctx, params)
}

// BindQueue binds a queue to a fanout exchange.
// The queue will receive all messages published to this exchange.
// The queue and exchange must be in the same namespace.
//
// Example:
//
//	err := fx.BindQueue(ctx, queueParams, exchangeParams)
func (fx *FanoutExchange) BindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return fx.fanoutStore.BindQueue(ctx, queueParams, exchangeParams)
}

// UnbindQueue removes a queue binding from a fanout exchange.
// The queue and exchange must be in the same namespace.
func (fx *FanoutExchange) UnbindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return fx.fanoutStore.UnbindQueue(ctx, queueParams, exchangeParams)
}

// MatchQueues returns all queues bound to this fanout exchange.
// Use this for message production to find target queues.
// This is equivalent to BoundQueues for fanout exchanges.
//
// Example:
//
//	queues, err := fx.MatchQueues(ctx, exchangeParams)
func (fx *FanoutExchange) MatchQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]queue.QueueParams, error) {
	return fx.fanoutStore.BoundQueues(ctx, exchangeParams)
}

// BoundQueues returns all queues bound to this fanout exchange.
func (fx *FanoutExchange) BoundQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]queue.QueueParams, error) {
	return fx.fanoutStore.BoundQueues(ctx, exchangeParams)
}
