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

	qm := redissmq.NewQueueManager()
	tasksQueue := queue.MustQueueParams(fmt.Sprintf("tasks-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, tasksQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	consumed := make(chan string, 3)
	c := redissmq.NewConsumer()
	c.Consume(tasksQueue, func(_ context.Context, m *message.Transferable) error {
		consumed <- m.Body.(string)
		return nil
	})
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	delayedMsg := message.New().
		SetBody("Delayed message").
		SetQueue(tasksQueue).
		SetScheduledDelay(2 * time.Second)
	ids, err := p.Produce(ctx, delayedMsg)
	if err != nil {
		return fmt.Errorf("produce delayed: %w", err)
	}
	log.Printf("Scheduled delayed message: %v", ids)

	immediateMsg := message.New().
		SetBody("Immediate message").
		SetQueue(tasksQueue)
	ids, err = p.Produce(ctx, immediateMsg)
	if err != nil {
		return fmt.Errorf("produce immediate: %w", err)
	}
	log.Printf("Produced immediate message: %v", ids)

	for i := 0; i < 2; i++ {
		select {
		case body := <-consumed:
			log.Printf("Consumed: %s", body)
		case <-time.After(15 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	result, err := qm.BrowseMessages(ctx, tasksQueue, &queue.BrowseParams{
		Filter: queue.BrowseScheduled,
	})
	if err != nil {
		return fmt.Errorf("browse scheduled: %w", err)
	}
	log.Printf("Scheduled messages remaining: %d", result.Total)

	log.Println("Scheduling example complete")
	return nil
}
