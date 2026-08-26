/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Batch acknowledgments for high throughput.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := redissmq.Init(ctx, redis.Options{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Create queue
	batchQueue := queue.MustQueueParams(fmt.Sprintf("batch-test-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, batchQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		log.Fatalf("create queue: %v", err)
	}

	// Consumer with batch acknowledgments
	// Flush every 50 messages or 2 seconds
	consumed := make(chan string, 100)
	c := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    50,
			BatchTimeout: 2 * time.Second,
		}),
	)

	c.Consume(batchQueue, func(ctx context.Context, m *message.Transferable) error {
		consumed <- m.ID
		return nil
	})
	c.Run(ctx)

	// Producer
	p := redissmq.NewProducer()
	p.Run(ctx)

	// Produce 100 messages
	start := time.Now()
	for i := 1; i <= 100; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Message #%d", i)).
			SetQueue(batchQueue)

		_, err := p.Produce(ctx, m)
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
	}

	// Consume all 100 messages
	for i := 0; i < 100; i++ {
		select {
		case id := <-consumed:
			if i == 0 || i == 99 {
				log.Printf("Consumed: %s", id)
			}
		case <-time.After(30 * time.Second):
			log.Fatalf("timeout after %d messages", i)
		}
	}

	elapsed := time.Since(start)
	log.Printf("100 messages in %v (%.0f msg/s)", elapsed, 100/elapsed.Seconds())
	log.Println("Batch example complete")
}
