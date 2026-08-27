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

// FanoutExchange manages fanout exchanges that broadcast to all bound queues.
type FanoutExchange interface {
	// Create creates a fanout exchange with the given queue policy.
	// Returns TypeMismatchError if params.Type() is not TypeFanout.
	Create(ctx context.Context, params *Params, policy ExchangePolicy) error

	// Delete removes a fanout exchange and all its queue bindings.
	// Returns ErrHasBoundQueues if any queues are still bound.
	Delete(ctx context.Context, params *Params) error

	// BindQueue binds a queue to a fanout exchange.
	// The queue will receive all messages published to this exchange.
	// The queue and exchange must be in the same namespace.
	BindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params) error

	// UnbindQueue removes a queue binding from a fanout exchange.
	// The queue and exchange must be in the same namespace.
	UnbindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params) error

	// MatchQueues returns all queues bound to this fanout exchange.
	// This is equivalent to BoundQueues for fanout exchanges.
	MatchQueues(ctx context.Context, exchangeParams *Params) ([]queue.Params, error)

	// BoundQueues returns all queues bound to this fanout exchange.
	BoundQueues(ctx context.Context, exchangeParams *Params) ([]queue.Params, error)
}
