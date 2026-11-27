/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Full producer and consumer in one process.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	redissmq "github.com/weyoss/go-redis-smq"
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

	ordersQueue := queue.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	consumed := make(chan string, 5)
	c := redissmq.NewConsumer()
	c.Consume(ordersQueue, func(_ context.Context, m *message.Transferable) error {
		consumed <- fmt.Sprintf("%v", m.Body)
		return nil
	})
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	for i := 1; i <= 5; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Order #%d", i)).
			SetQueue(ordersQueue)
		ids, err := p.Produce(ctx, m)
		if err != nil {
			return fmt.Errorf("produce: %w", err)
		}
		log.Printf("Produced: %v", ids)
	}

	for i := 0; i < 5; i++ {
		select {
		case body := <-consumed:
			log.Printf("Consumed: %s", body)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	log.Println("All messages processed")
	return nil
}
