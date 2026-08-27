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

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	if err := redissmq.Init(ctx, rdb); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	//
	qm := redissmq.NewQueueManager()

	// Create queues
	ordersQueue := queue.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	qm.Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	eventsQueue := queue.MustQueueParams(fmt.Sprintf("events-%d", time.Now().UnixMilli()))
	qm.Create(ctx, eventsQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Create direct exchange
	dx := redissmq.NewDirectExchange()
	exchangeParams := exchange.MustExchangeParams("app-events", exchange.TypeDirect)

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
	orderConsumer.Consume(ordersQueue, func(ctx context.Context, m *message.Transferable) error {
		orderCh <- fmt.Sprintf("Orders queue: %v", m.Body)
		return nil
	})
	orderConsumer.Run(ctx)

	eventCh := make(chan string, 1)
	eventConsumer := redissmq.NewConsumer()
	eventConsumer.Consume(eventsQueue, func(ctx context.Context, m *message.Transferable) error {
		eventCh <- fmt.Sprintf("Events queue: %v", m.Body)
		return nil
	})
	eventConsumer.Run(ctx)

	// Produce via exchange
	p := redissmq.NewProducer()
	p.Run(ctx)

	m := message.New().
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
