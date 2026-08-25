/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package exchange provides the public API for managing RedisSMQ exchanges.
//
// Exchanges route messages from producers to one or more queues. This package
// defines the exchange types, parameters, policies, errors, and the public
// interfaces for direct, topic, and fanout exchanges.
//
// The concrete implementations are provided by the root redissmq package and
// are created using factory functions such as redissmq.NewDirectExchange().
//
// Example:
//
//	dx := redissmq.NewDirectExchange()
//	params := exchange.MustExchangeParams("orders", exchange.TypeDirect)
//	err := dx.Create(ctx, params, exchange.PolicyStandard)
//	err = dx.BindQueue(ctx, queueParams, params, "order.created")
//
// The package includes:
//
//   - ExchangeType: the type of routing (direct, fanout, topic)
//   - ExchangeParams: identifies an exchange and its type
//   - ExchangeProps: stored configuration of an exchange
//   - ExchangePolicy: restricts which queue types can bind
//   - Errors: typed errors for common failures
//   - Interfaces: Manager, DirectExchange, FanoutExchange, TopicExchange
package exchange
