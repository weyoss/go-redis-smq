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

	userQueue := queue.MustQueueParams(fmt.Sprintf("user-events-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, userQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create user queue: %w", err)
	}

	orderQueue := queue.MustQueueParams(fmt.Sprintf("order-events-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, orderQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create order queue: %w", err)
	}

	allQueue := queue.MustQueueParams(fmt.Sprintf("all-events-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, allQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create all queue: %w", err)
	}

	tx := redissmq.NewTopicExchange()
	exchangeParams := exchange.MustExchangeParams(fmt.Sprintf("app-events-%d", time.Now().UnixMilli()), exchange.TypeTopic)

	// Bind queues with patterns:
	// "user.*" matches user.created, user.updated, user.deleted
	// "order.#" matches order.created, order.items.added, order.paid
	// "#" matches everything
	if err := tx.BindQueue(ctx, userQueue, exchangeParams, "user.*"); err != nil {
		return fmt.Errorf("bind user queue: %w", err)
	}
	if err := tx.BindQueue(ctx, orderQueue, exchangeParams, "order.#"); err != nil {
		return fmt.Errorf("bind order queue: %w", err)
	}
	if err := tx.BindQueue(ctx, allQueue, exchangeParams, "#"); err != nil {
		return fmt.Errorf("bind all queue: %w", err)
	}

	patterns, err := tx.Patterns(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("list patterns: %w", err)
	}
	log.Printf("Patterns: %v", patterns)

	type result struct {
		queue string
		body  interface{}
	}
	ch := make(chan result, 10)

	startConsumer := func(q *queue.Params, label string) error {
		c := redissmq.NewConsumer()
		c.Consume(q, func(_ context.Context, m *message.Transferable) error {
			ch <- result{queue: label, body: m.Body}
			return nil
		})
		if err := c.Run(ctx); err != nil {
			return fmt.Errorf("run %s consumer: %w", label, err)
		}
		return nil
	}

	if err := startConsumer(userQueue, "user-events"); err != nil {
		return err
	}
	if err := startConsumer(orderQueue, "order-events"); err != nil {
		return err
	}
	if err := startConsumer(allQueue, "all-events"); err != nil {
		return err
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	// Test 1: user.created — matches "user.*" and "#"
	m := message.New().
		SetBody("User created").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("user.created")
	ids, err := p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce user.created: %w", err)
	}
	log.Printf("'user.created' → %d queue(s): %v", len(ids), ids)

	// Test 2: order.created — matches "order.#" and "#"
	m = message.New().
		SetBody("Order created").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("order.created")
	ids, err = p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce order.created: %w", err)
	}
	log.Printf("'order.created' → %d queue(s): %v", len(ids), ids)

	// Test 3: system.restart — matches only "#"
	m = message.New().
		SetBody("System restart").
		SetTopicExchange(exchangeParams).
		SetExchangeRoutingKey("system.restart")
	ids, err = p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce system.restart: %w", err)
	}
	log.Printf("'system.restart' → %d queue(s): %v", len(ids), ids)

	for i := 0; i < 5; i++ {
		select {
		case r := <-ch:
			log.Printf("[%s] Received: %v", r.queue, r.body)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	bindings, err := tx.Bindings(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("list bindings: %w", err)
	}
	for pattern, queues := range bindings {
		names := make([]string, len(queues))
		for i, q := range queues {
			names[i] = q.String()
		}
		log.Printf("  %s → %v", pattern, names)
	}

	log.Println("Topic exchange example complete")
	return nil
}
