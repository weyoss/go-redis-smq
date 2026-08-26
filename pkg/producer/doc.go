/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package producer provides the public API for creating and managing
// RedisSMQ producers.
//
// A producer is responsible for publishing messages to queues or exchanges.
// It maintains its own lifecycle, supports automatic shutdown on context
// cancellation, and exposes methods to produce messages directly or through
// exchanges.
//
// # Concrete Implementation
//
// The package defines the Producer interface and the sentinel errors that
// production operations may return. The concrete implementation is provided
// by the root redissmq package and is created using redissmq.NewProducer().
//
// # Lifecycle
//
// A producer must be started with Run(ctx) before it can publish messages.
// The provided context is used to control the producer's lifetime; cancelling
// the context triggers a graceful shutdown. Shutdown can also be called
// explicitly and is safe to call multiple times.
//
// # Publishing Messages
//
// To publish a message, create a ProducibleMessage using message.New(),
// configure it, and pass it to Produce:
//
//	prod := redissmq.NewProducer()
//	if err := prod.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer prod.Shutdown(ctx)
//
//	m := message.New().
//	    SetBody("hello").
//	    SetQueue(queueParams)
//
//	ids, err := prod.Produce(ctx, m)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Exchanges
//
// In addition to direct queue delivery, a producer can publish via exchanges:
//
//	direct:  message.SetDirectExchange(exchangeParams).SetExchangeRoutingKey("key")
//	topic:   message.SetTopicExchange(exchangeParams).SetExchangeRoutingKey("pattern")
//	fanout:  message.SetFanoutExchange(exchangeParams)
//
// # Events
//
// Producers emit public events that can be subscribed to using the functions
// in this package (e.g., SubscribeUp, SubscribeMessagePublished). These events
// are delivered over the public user event bus, which is started by calling
// redissmq.InitUserEventBus(ctx).
//
// # Example
//
//	prod := redissmq.NewProducer()
//	if err := prod.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer prod.Shutdown(ctx)
//
//	m := message.New().
//	    SetBody(map[string]interface{}{"orderId": 123}).
//	    SetQueue(ordersQueue)
//
//	ids, err := prod.Produce(ctx, m)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Published:", ids)
package producer
