/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Pub/Sub with consumer groups.
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

	notifQueue := queue.MustQueueParams(fmt.Sprintf("notifications-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, notifQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		return fmt.Errorf("create queue: %w", err)
	}

	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, notifQueue, "email-service"); err != nil {
		return fmt.Errorf("create email group: %w", err)
	}
	if _, err := cgm.Save(ctx, notifQueue, "sms-service"); err != nil {
		return fmt.Errorf("create sms group: %w", err)
	}

	emailCh := make(chan string, 5)
	emailConsumer := redissmq.NewConsumer()
	emailConsumer.ConsumeWithGroup(notifQueue, "email-service", func(_ context.Context, m *message.Transferable) error {
		emailCh <- fmt.Sprintf("Email received: %v", m.Body)
		return nil
	})
	if err := emailConsumer.Run(ctx); err != nil {
		return fmt.Errorf("run email consumer: %w", err)
	}

	smsCh := make(chan string, 5)
	smsConsumer := redissmq.NewConsumer()
	smsConsumer.ConsumeWithGroup(notifQueue, "sms-service", func(_ context.Context, m *message.Transferable) error {
		smsCh <- fmt.Sprintf("SMS received: %v", m.Body)
		return nil
	})
	if err := smsConsumer.Run(ctx); err != nil {
		return fmt.Errorf("run sms consumer: %w", err)
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	m := message.New().
		SetBody("System maintenance at 2 AM").
		SetQueue(notifQueue)

	ids, err := p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	log.Printf("Produced: %v (expecting %d IDs)", ids, 2)

	for i := 0; i < 2; i++ {
		select {
		case msg := <-emailCh:
			log.Println(msg)
		case msg := <-smsCh:
			log.Println(msg)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	log.Println("Pub/Sub example complete")
	return nil
}
