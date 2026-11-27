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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer func() { _ = rdb.Close() }()

	if err := redissmq.Init(ctx, rdb); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	defer redissmq.Shutdown()

	qm := redissmq.NewQueueManager()

	ordersQueue := queue.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create orders queue: %w", err)
	}

	eventsQueue := queue.MustQueueParams(fmt.Sprintf("events-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, eventsQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create events queue: %w", err)
	}

	dx := redissmq.NewDirectExchange()
	exchangeParams := exchange.MustExchangeParams("app-events", exchange.TypeDirect)

	if err := dx.BindQueue(ctx, ordersQueue, exchangeParams, "order.created"); err != nil {
		return fmt.Errorf("bind orders: %w", err)
	}
	if err := dx.BindQueue(ctx, eventsQueue, exchangeParams, "order.created"); err != nil {
		return fmt.Errorf("bind events: %w", err)
	}

	orderCh := make(chan string, 1)
	orderConsumer := redissmq.NewConsumer()
	orderConsumer.Consume(ordersQueue, func(_ context.Context, m *message.Transferable) error {
		orderCh <- fmt.Sprintf("Orders queue: %v", m.Body)
		return nil
	})
	if err := orderConsumer.Run(ctx); err != nil {
		return fmt.Errorf("run orders consumer: %w", err)
	}

	eventCh := make(chan string, 1)
	eventConsumer := redissmq.NewConsumer()
	eventConsumer.Consume(eventsQueue, func(_ context.Context, m *message.Transferable) error {
		eventCh <- fmt.Sprintf("Events queue: %v", m.Body)
		return nil
	})
	if err := eventConsumer.Run(ctx); err != nil {
		return fmt.Errorf("run events consumer: %w", err)
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	m := message.New().
		SetBody(map[string]interface{}{"orderId": 123}).
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created")

	ids, err := p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	log.Printf("Produced to %d queue(s): %v", len(ids), ids)

	for i := 0; i < 2; i++ {
		select {
		case msg := <-orderCh:
			log.Println(msg)
		case msg := <-eventCh:
			log.Println(msg)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	log.Println("Exchange example complete")
	return nil
}
