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
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx := context.Background()

	// Initialize
	if err := redissmq.Init(ctx, redis.Options{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Create a queue
	ordersQueue := queue.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		log.Fatalf("create queue: %v", err)
	}

	// Start producer
	producer := redissmq.NewProducer()
	if err := producer.Run(ctx); err != nil {
		log.Fatalf("run producer: %v", err)
	}
	defer producer.Shutdown(ctx)

	// Produce messages
	for i := 1; i <= 5; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Order #%d", i)).
			SetQueue(ordersQueue).
			SetTTL(5 * time.Minute).
			SetRetryThreshold(3).
			SetRetryDelay(10 * time.Second)

		ids, err := producer.Produce(ctx, m)
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Printf("Produced: %v", ids)
	}
}
