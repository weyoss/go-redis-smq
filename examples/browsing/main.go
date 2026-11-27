/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Browse queue messages by category.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/config"
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

	// Enable message audit so we can browse acknowledged/dead-lettered messages
	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 1000
	if _, err := redissmq.NewConfigManager().Save(ctx, cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	qm := redissmq.NewQueueManager()

	browseQueue := queue.MustQueueParams(fmt.Sprintf("browse-test-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, browseQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	for i := 1; i <= 10; i++ {
		m := message.New().
			SetBody(fmt.Sprintf("Message #%d", i)).
			SetQueue(browseQueue)
		ids, err := p.Produce(ctx, m)
		if err != nil {
			return fmt.Errorf("produce: %w", err)
		}
		log.Printf("Produced: %v", ids)
	}

	// Produce a scheduled message
	scheduledMsg := message.New().
		SetBody("Scheduled message").
		SetQueue(browseQueue).
		SetScheduledDelay(1 * time.Hour)
	ids, err := p.Produce(ctx, scheduledMsg)
	if err != nil {
		return fmt.Errorf("produce scheduled: %w", err)
	}
	log.Printf("Produced scheduled: %v", ids)

	// Consume 5 messages (they'll be acknowledged)
	c := redissmq.NewConsumer()
	consumed := make(chan string, 5)
	c.Consume(browseQueue, func(_ context.Context, m *message.Transferable) error {
		consumed <- m.ID
		return nil
	})
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	for i := 0; i < 5; i++ {
		select {
		case id := <-consumed:
			log.Printf("Consumed: %s", id)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	time.Sleep(500 * time.Millisecond) // let acks settle

	for _, p := range []struct {
		label  string
		filter queue.BrowseFilter
	}{
		{"Published", queue.BrowsePublished},
		{"Pending", queue.BrowsePending},
		{"Scheduled", queue.BrowseScheduled},
		{"Acknowledged", queue.BrowseAcknowledged},
		{"Dead-Lettered", queue.BrowseDeadLettered},
	} {
		if err := printBrowseResult(ctx, p.label, browseQueue, p.filter); err != nil {
			return err
		}
	}

	fmt.Println("\n=== Pagination ===")
	offset := int64(0)
	pageSize := int64(3)
	page := 1
	for {
		result, err := qm.BrowseMessages(ctx, browseQueue, &queue.BrowseParams{
			Filter: queue.BrowsePublished,
			Offset: offset,
			Count:  pageSize,
		})
		if err != nil {
			return fmt.Errorf("browse: %w", err)
		}

		fmt.Printf("Page %d [%d-%d] of %d:\n", page, offset, offset+result.Count-1, result.Total)
		for _, id := range result.IDs {
			fmt.Printf("  - %s\n", id)
		}

		if !result.HasMore {
			break
		}
		offset += result.Count
		page++
	}

	log.Println("Browsing example complete")
	return nil
}

func printBrowseResult(ctx context.Context, label string, queueParams *queue.Params, filter queue.BrowseFilter) error {
	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, queueParams, &queue.BrowseParams{
		Filter: filter,
		Offset: 0,
		Count:  100,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	fmt.Printf("\n%s: %d/%d\n", label, len(result.IDs), result.Total)
	for _, id := range result.IDs {
		fmt.Printf("  - %s\n", id)
	}
	return nil
}
