/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Scheduled and delayed messages.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/weyoss/go-redis-smq"
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

	// Create queue
	tasksQueue := q.MustQueueParams(fmt.Sprintf("tasks-%d", time.Now().UnixMilli()))
	if err := queue.Create(ctx, tasksQueue, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
		log.Fatalf("create queue: %v", err)
	}

	// Consumer
	consumed := make(chan string, 3)
	c := redissmq.NewConsumer()
	c.Consume(tasksQueue, func(ctx context.Context, m *msg.Transferable) error {
		consumed <- m.Body.(string)
		return nil
	})
	c.Run(ctx)

	// Producer
	p := redissmq.NewProducer()
	p.Run(ctx)

	// Message 1: Delayed by 2 seconds
	delayedMsg := msg.New().
		SetBody("Delayed message").
		SetQueue(tasksQueue).
		SetScheduledDelay(2 * time.Second)

	ids, err := p.Produce(ctx, delayedMsg)
	if err != nil {
		log.Fatalf("produce delayed: %v", err)
	}
	log.Printf("Scheduled delayed message: %v", ids)

	// Message 2: Immediate (no delay)
	immediateMsg := msg.New().
		SetBody("Immediate message").
		SetQueue(tasksQueue)

	ids, err = p.Produce(ctx, immediateMsg)
	if err != nil {
		log.Fatalf("produce immediate: %v", err)
	}
	log.Printf("Produced immediate message: %v", ids)

	// Expect 2 messages
	for i := 0; i < 2; i++ {
		select {
		case body := <-consumed:
			log.Printf("Consumed: %s", body)
		case <-time.After(15 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	// Check scheduled messages
	result, err := queue.BrowseMessages(ctx, tasksQueue, &q.BrowseParams{
		Filter: q.BrowseScheduled,
	})
	if err != nil {
		log.Fatalf("browse scheduled: %v", err)
	}
	log.Printf("Scheduled messages remaining: %d", result.Total)

	log.Println("Scheduling example complete")
}
