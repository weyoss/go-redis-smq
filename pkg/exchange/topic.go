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

// TopicExchange manages topic exchanges with pattern-based routing.
//
// Topic exchanges route messages to queues based on pattern matching between
// routing keys and binding patterns. Uses AMQP-style wildcards:
//   - "*" matches exactly one dot-separated token
//   - "#" matches zero or more dot-separated tokens
//
// Example:
//
//	tx := redissmq.NewTopicExchange()
//	params := exchange.MustExchangeParams("events", exchange.TypeTopic)
//	tx.Create(ctx, params, exchange.PolicyStandard)
//	tx.BindQueue(ctx, queueParams, params, "order.#")
//	queues, _ := tx.MatchQueues(ctx, params, "order.created")
type TopicExchange interface {
	// Create creates a topic exchange with the given queue policy.
	// Returns TypeMismatchError if params.Type() is not TypeTopic.
	Create(ctx context.Context, params *Params, policy ExchangePolicy) error

	// Delete removes a topic exchange and all its pattern bindings.
	// Returns ErrHasBoundQueues if any patterns still have bound queues.
	Delete(ctx context.Context, params *Params) error

	// BindQueue binds a queue to a topic exchange with a binding pattern.
	// The pattern must be a valid AMQP-style topic pattern.
	// The queue and exchange must be in the same namespace.
	//
	// Valid patterns:
	//   - "order.*"     matches "order.created", "order.cancelled"
	//   - "order.#"     matches "order.created", "order.items.added"
	//   - "#"           matches all routing keys
	//   - "*.created"   matches "order.created", "user.created"
	BindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params, pattern string) error

	// UnbindQueue removes a queue binding from a specific pattern.
	// The queue and exchange must be in the same namespace.
	UnbindQueue(ctx context.Context, queueParams *queue.Params, exchangeParams *Params, pattern string) error

	// MatchQueues returns all queues whose binding patterns match the routing key.
	MatchQueues(ctx context.Context, exchangeParams *Params, routingKey string) ([]queue.Params, error)

	// Patterns returns all binding patterns registered for this topic exchange.
	Patterns(ctx context.Context, exchangeParams *Params) ([]string, error)

	// BoundQueues returns all queues bound to a specific pattern.
	BoundQueues(ctx context.Context, exchangeParams *Params, pattern string) ([]queue.Params, error)

	// Bindings returns all pattern to queue mappings for this exchange.
	Bindings(ctx context.Context, exchangeParams *Params) (map[string][]queue.Params, error)
}
