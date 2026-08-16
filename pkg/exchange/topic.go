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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
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
//	tx := exchange.NewTopicExchange()
//	tx.Create(ctx, params, x.PolicyStandard)
//	tx.BindQueue(ctx, queueParams, exchangeParams, "order.#")
//	queues, _ := tx.MatchQueues(ctx, exchangeParams, "order.created")
type TopicExchange struct {
	store      *internalExchange.Store
	topicStore *internalExchange.TopicStore
}

// NewTopicExchange creates a new topic exchange manager.
// It uses the default Redis-backed internal manager.
func NewTopicExchange() *TopicExchange {
	manager := internalExchange.NewManager()
	return &TopicExchange{
		store:      manager.Store(),
		topicStore: manager.Topic(),
	}
}

// Create creates a topic exchange with the given queue policy.
// Returns TypeMismatchError if params.Type() is not TypeTopic.
func (tx *TopicExchange) Create(ctx context.Context, params *x.ExchangeParams, policy x.ExchangePolicy) error {
	if params.Type() != x.TypeTopic {
		return x.NewTypeMismatchError(x.TypeTopic, params.Type())
	}
	return tx.store.Save(ctx, params, policy)
}

// Delete removes a topic exchange and all its pattern bindings.
// Returns ErrHasBoundQueues if any patterns still have bound queues.
func (tx *TopicExchange) Delete(ctx context.Context, params *x.ExchangeParams) error {
	return tx.topicStore.Delete(ctx, params)
}

// BindQueue binds a queue to a topic exchange with a binding pattern.
// The pattern must be a valid AMQP-style topic pattern.
// The queue and exchange must be in the same namespace.
//
// Valid patterns:
//   - "order.*"     matches "order.created", "order.cancelled"
//   - "order.#"     matches "order.created", "order.items.added"
//   - "#"           matches all routing keys
//   - "*.created"   matches "order.created", "user.created"
//
// Example:
//
//	err := tx.BindQueue(ctx, queueParams, exchangeParams, "order.#")
func (tx *TopicExchange) BindQueue(
	ctx context.Context,
	queueParams *q.QueueParams,
	exchangeParams *x.ExchangeParams,
	pattern string,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return tx.topicStore.BindQueue(ctx, queueParams, exchangeParams, pattern)
}

// UnbindQueue removes a queue binding from a specific pattern.
// The queue and exchange must be in the same namespace.
func (tx *TopicExchange) UnbindQueue(
	ctx context.Context,
	queueParams *q.QueueParams,
	exchangeParams *x.ExchangeParams,
	pattern string,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return x.ErrNamespaceMismatch
	}
	return tx.topicStore.UnbindQueue(ctx, queueParams, exchangeParams, pattern)
}

// MatchQueues returns all queues whose binding patterns match the routing key.
// Use this for message production to find target queues.
//
// Example:
//
//	// With pattern "order.#" bound, routing key "order.created" matches
//	queues, err := tx.MatchQueues(ctx, exchangeParams, "order.created")
func (tx *TopicExchange) MatchQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) ([]q.QueueParams, error) {
	return tx.topicStore.MatchQueues(ctx, exchangeParams, routingKey)
}

// Patterns returns all binding patterns registered for this topic exchange.
//
// Example:
//
//	patterns, err := tx.Patterns(ctx, exchangeParams)
//	// patterns: ["order.#", "user.*", "#"]
func (tx *TopicExchange) Patterns(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]string, error) {
	return tx.topicStore.Patterns(ctx, exchangeParams)
}

// BoundQueues returns all queues bound to a specific pattern.
//
// Example:
//
//	queues, err := tx.BoundQueues(ctx, exchangeParams, "order.#")
func (tx *TopicExchange) BoundQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	pattern string,
) ([]q.QueueParams, error) {
	return tx.topicStore.BoundQueues(ctx, exchangeParams, pattern)
}

// Bindings returns all pattern to queue mappings for this exchange.
//
// Example:
//
//	bindings, err := tx.Bindings(ctx, exchangeParams)
//	for pattern, queues := range bindings {
//	    fmt.Printf("%s: %d queues\n", pattern, len(queues))
//	}
func (tx *TopicExchange) Bindings(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) (map[string][]q.QueueParams, error) {
	return tx.topicStore.Bindings(ctx, exchangeParams)
}
