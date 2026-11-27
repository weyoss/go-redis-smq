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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
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

	batchQueue := queue.MustQueueParams(fmt.Sprintf("batch-test-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, batchQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create queue: %w", err)
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
	c.Consume(batchQueue, func(_ context.Context, m *message.Transferable) error {
		consumed <- m.ID
		return nil
	})
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	// Producer
	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	// Produce 100 messages
	start := time.Now()
	for i := 1; i <= 100; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Message #%d", i)).
			SetQueue(batchQueue)
		if _, err := p.Produce(ctx, m); err != nil {
			return fmt.Errorf("produce: %w", err)
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
			return fmt.Errorf("timeout after %d messages", i)
		}
	}

	elapsed := time.Since(start)
	log.Printf("100 messages in %v (%.0f msg/s)", elapsed, 100/elapsed.Seconds())
	log.Println("Batch example complete")
	return nil
}
