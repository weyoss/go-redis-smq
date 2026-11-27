/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Produce messages to a queue.
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

	producer := redissmq.NewProducer()
	if err := producer.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	for i := 1; i <= 5; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Order #%d", i)).
			SetQueue(ordersQueue).
			SetTTL(5 * time.Minute).
			SetRetryThreshold(3).
			SetRetryDelay(10 * time.Second)

		ids, err := producer.Produce(ctx, m)
		if err != nil {
			return fmt.Errorf("produce: %w", err)
		}
		log.Printf("Produced: %v", ids)
	}

	return nil
}
