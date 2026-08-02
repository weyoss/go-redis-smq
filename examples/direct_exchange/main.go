/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Exchange-based routing.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Create queues
	ordersQueue := q.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	queue.Create(ctx, ordersQueue, q.TypeFIFO, q.DeliveryPointToPoint)

	eventsQueue := q.MustQueueParams(fmt.Sprintf("events-%d", time.Now().UnixMilli()))
	queue.Create(ctx, eventsQueue, q.TypeFIFO, q.DeliveryPointToPoint)

	// Create direct exchange
	dx := exchange.NewDirectExchange(nil)
	exchangeParams := x.MustExchangeParams("app-events", x.TypeDirect)

	// Bind queues to routing keys
	if err := dx.BindQueue(ctx, ordersQueue, exchangeParams, "order.created"); err != nil {
		log.Fatalf("bind orders: %v", err)
	}
	if err := dx.BindQueue(ctx, eventsQueue, exchangeParams, "order.created"); err != nil {
		log.Fatalf("bind events: %v", err)
	}

	// Consumers
	orderCh := make(chan string, 1)
	orderConsumer := redissmq.NewConsumer()
	orderConsumer.Consume(ordersQueue, func(ctx context.Context, m *msg.Transferable) error {
		orderCh <- fmt.Sprintf("Orders queue: %v", m.Body)
		return nil
	})
	orderConsumer.Run(ctx)

	eventCh := make(chan string, 1)
	eventConsumer := redissmq.NewConsumer()
	eventConsumer.Consume(eventsQueue, func(ctx context.Context, m *msg.Transferable) error {
		eventCh <- fmt.Sprintf("Events queue: %v", m.Body)
		return nil
	})
	eventConsumer.Run(ctx)

	// Produce via exchange
	p := redissmq.NewProducer()
	p.Run(ctx)

	m := msg.New().
		SetBody(map[string]interface{}{"orderId": 123}).
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created")

	ids, err := p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("Produced to %d queue(s): %v", len(ids), ids)

	// Both queues should receive the message
	for i := 0; i < 2; i++ {
		select {
		case msg := <-orderCh:
			log.Println(msg)
		case msg := <-eventCh:
			log.Println(msg)
		case <-time.After(10 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	log.Println("Exchange example complete")
}
