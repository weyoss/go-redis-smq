/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Consume messages from a queue.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize
	if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Create a queue (if not exists)
	ordersQueue := q.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	if err := queue.Create(ctx, ordersQueue, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
		// Queue may already exist — continue
		log.Printf("create queue: %v (may already exist)", err)
	}

	// Start consumer
	c := redissmq.NewConsumer(
		consumer.WithHeartbeatTTL(30 * time.Second),
	)

	c.Consume(ordersQueue, func(ctx context.Context, m *msg.Transferable) error {
		log.Printf("Received: %v (ID: %s)", m.Body, m.ID)
		return nil
	})

	if err := c.Run(ctx); err != nil {
		log.Fatalf("run consumer: %v", err)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
}
