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

	ordersQueue := queue.MustQueueParams(fmt.Sprintf("orders-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		log.Printf("create queue: %v (may already exist)", err)
	}

	c := redissmq.NewConsumer(
		consumer.WithHeartbeatTTL(30 * time.Second),
	)
	c.Consume(ordersQueue, func(_ context.Context, m *message.Transferable) error {
		log.Printf("Received: %v (ID: %s)", m.Body, m.ID)
		return nil
	})
	if err := c.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	return nil
}
