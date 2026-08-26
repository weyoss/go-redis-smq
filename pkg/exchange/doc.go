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
// Exchanges route messages from producers to one or more queues. The package
// defines the exchange types, parameters, policies, errors, and the public
// interfaces for direct, topic, and fanout exchanges.
//
// # Concrete Implementations
//
// The package itself contains only interfaces and data types. Concrete
// implementations are provided by the root redissmq package and are created
// using the following factory functions:
//
//	redissmq.NewExchangeManager()  // returns an exchange.Manager
//	redissmq.NewDirectExchange()   // returns an exchange.DirectExchange
//	redissmq.NewFanoutExchange()   // returns an exchange.FanoutExchange
//	redissmq.NewTopicExchange()    // returns an exchange.TopicExchange
//
// # Exchange Types
//
// The package supports three exchange types, represented by ExchangeType:
//
//   - TypeDirect: routes messages to queues with an exact matching routing key.
//   - TypeTopic: routes messages using AMQP-style pattern matching (* and #).
//   - TypeFanout: broadcasts messages to all bound queues, ignoring routing keys.
//
// # Policies
//
// Each exchange can enforce a queue policy via ExchangePolicy:
//
//   - PolicyStandard: allows only FIFO and LIFO queues.
//   - PolicyPriority: allows only priority queues.
//
// # Example
//
//	dx := redissmq.NewDirectExchange()
//	params := exchange.MustExchangeParams("orders", exchange.TypeDirect)
//	if err := dx.Create(ctx, params, exchange.PolicyStandard); err != nil {
//	    log.Fatal(err)
//	}
//	if err := dx.BindQueue(ctx, queueParams, params, "order.created"); err != nil {
//	    log.Fatal(err)
//	}
package exchange
