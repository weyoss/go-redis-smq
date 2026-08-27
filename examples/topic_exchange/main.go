/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Topic exchange with pattern-based routing.
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
	userQueue := queue.MustQueueParams(fmt.Sprintf("user-events-%d", time.Now().UnixMilli()))
	qm.Create(ctx, userQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	orderQueue := queue.MustQueueParams(fmt.Sprintf("order-events-%d", time.Now().UnixMilli()))
	qm.Create(ctx, orderQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	allQueue := queue.MustQueueParams(fmt.Sprintf("all-events-%d", time.Now().UnixMilli()))
	qm.Create(ctx, allQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Create topic exchange
	tx := redissmq.NewTopicExchange()
	exchangeParams := exchange.MustExchangeParams(fmt.Sprintf("app-events-%d", time.Now().UnixMilli()), exchange.TypeTopic)

	// Bind queues with patterns:
	// "user.*" matches user.created, user.updated, user.deleted
	// "order.#" matches order.created, order.items.added, order.paid
	// "#" matches everything
	if err := tx.BindQueue(ctx, userQueue, exchangeParams, "user.*"); err != nil {
		log.Fatalf("bind user queue: %v", err)
	}
	if err := tx.BindQueue(ctx, orderQueue, exchangeParams, "order.#"); err != nil {
		log.Fatalf("bind order queue: %v", err)
	}
	if err := tx.BindQueue(ctx, allQueue, exchangeParams, "#"); err != nil {
		log.Fatalf("bind all queue: %v", err)
	}

	// Show patterns
	patterns, _ := tx.Patterns(ctx, exchangeParams)
	log.Printf("Patterns: %v", patterns)

	// Consumers
	type result struct {
		queue string
		body  interface{}
	}
	ch := make(chan result, 10)

	startConsumer := func(queue *queue.Params, label string) {
		c := redissmq.NewConsumer()
		c.Consume(queue, func(ctx context.Context, m *message.Transferable) error {
			ch <- result{queue: label, body: m.Body}
			return nil
		})
		c.Run(ctx)
	}

	startConsumer(userQueue, "user-events")
	startConsumer(orderQueue, "order-events")
	startConsumer(allQueue, "all-events")

	// Producer
	p := redissmq.NewProducer()
	p.Run(ctx)

	// Test 1: user.created — matches "user.*" and "#"
	m := message.New().
		SetBody("User created").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("user.created")

	ids, err := p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("'user.created' → %d queue(s): %v", len(ids), ids)

	// Test 2: order.created — matches "order.#" and "#"
	m = message.New().
		SetBody("Order created").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("order.created")

	ids, err = p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("'order.created' → %d queue(s): %v", len(ids), ids)

	// Test 3: system.restart — matches only "#"
	m = message.New().
		SetBody("System restart").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("system.restart")

	ids, err = p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("'system.restart' → %d queue(s): %v", len(ids), ids)

	// Collect results
	for i := 0; i < 5; i++ {
		select {
		case r := <-ch:
			log.Printf("[%s] Received: %v", r.queue, r.body)
		case <-time.After(10 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	// Show bindings
	bindings, _ := tx.Bindings(ctx, exchangeParams)
	for pattern, queues := range bindings {
		names := make([]string, len(queues))
		for i, q := range queues {
			names[i] = q.String()
		}
		log.Printf("  %s → %v", pattern, names)
	}

	log.Println("Topic exchange example complete")
}
