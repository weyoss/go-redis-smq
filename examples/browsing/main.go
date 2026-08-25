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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Enable message audit so we can browse acknowledged/dead-lettered messages
	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 1000
	if _, err := config.Save(ctx, cfg); err != nil {
		log.Fatalf("save config: %v", err)
	}

	//
	qm := redissmq.NewQueueManager()

	// Create queue
	browseQueue := queue.MustQueueParams(fmt.Sprintf("browse-test-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, browseQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		log.Fatalf("create queue: %v", err)
	}

	// Produce some messages
	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		log.Fatalf("run producer: %v", err)
	}

	for i := 1; i <= 10; i++ {
		m := msg.New().
			SetBody(fmt.Sprintf("Message #%d", i)).
			SetQueue(browseQueue)

		ids, err := p.Produce(ctx, m)
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		log.Printf("Produced: %v", ids)
	}

	// Produce a scheduled message
	scheduledMsg := msg.New().
		SetBody("Scheduled message").
		SetQueue(browseQueue).
		SetScheduledDelay(1 * time.Hour)

	ids, err := p.Produce(ctx, scheduledMsg)
	if err != nil {
		log.Fatalf("produce scheduled: %v", err)
	}
	log.Printf("Produced scheduled: %v", ids)

	// Consume 5 messages (they'll be acknowledged)
	c := redissmq.NewConsumer()
	consumed := make(chan string, 5)
	c.Consume(browseQueue, func(ctx context.Context, m *msg.Transferable) error {
		consumed <- m.ID
		return nil
	})
	if err := c.Run(ctx); err != nil {
		log.Fatalf("run consumer: %v", err)
	}

	for i := 0; i < 5; i++ {
		select {
		case id := <-consumed:
			log.Printf("Consumed: %s", id)
		case <-time.After(10 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	time.Sleep(500 * time.Millisecond) // Let acks settle

	// ── Browse Messages ──

	// Published: All messages in the queue
	printBrowseResult(ctx, "Published", browseQueue, queue.BrowsePublished)

	// Pending: Messages waiting to be consumed
	printBrowseResult(ctx, "Pending", browseQueue, queue.BrowsePending)

	// Scheduled: Messages waiting for future delivery
	printBrowseResult(ctx, "Scheduled", browseQueue, queue.BrowseScheduled)

	// Acknowledged: Successfully processed messages (requires audit)
	printBrowseResult(ctx, "Acknowledged", browseQueue, queue.BrowseAcknowledged)

	// Dead-Lettered: Failed messages (requires audit)
	printBrowseResult(ctx, "Dead-Lettered", browseQueue, queue.BrowseDeadLettered)

	// ── Pagination ──

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
			log.Fatalf("browse: %v", err)
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
}

func printBrowseResult(ctx context.Context, label string, queueParams *queue.QueueParams, filter queue.BrowseFilter) {
	qm := redissmq.NewQueueManager()
	result, err := qm.BrowseMessages(ctx, queueParams, &queue.BrowseParams{
		Filter: filter,
		Offset: 0,
		Count:  100,
	})
	if err != nil {
		fmt.Printf("%s: error: %v\n", label, err)
		return
	}
	fmt.Printf("\n%s: %d/%d\n", label, len(result.IDs), result.Total)
	for _, id := range result.IDs {
		fmt.Printf("  - %s\n", id)
	}
}
