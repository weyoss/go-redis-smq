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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	// Create Pub/Sub queue
	notifQueue := queue.MustQueueParams(fmt.Sprintf("notifications-%d", time.Now().UnixMilli()))
	if err := redissmq.NewQueueManager().Create(ctx, notifQueue, queue.TypeFIFO, queue.DeliveryPubSub); err != nil {
		log.Fatalf("create queue: %v", err)
	}

	// Create consumer groups
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, notifQueue, "email-service"); err != nil {
		log.Fatalf("create email group: %v", err)
	}
	if _, err := cgm.Save(ctx, notifQueue, "sms-service"); err != nil {
		log.Fatalf("create sms group: %v", err)
	}

	// Consumer: Email service
	emailCh := make(chan string, 5)
	emailConsumer := redissmq.NewConsumer()
	emailConsumer.ConsumeWithGroup(notifQueue, "email-service", func(ctx context.Context, m *message.Transferable) error {
		emailCh <- fmt.Sprintf("Email received: %v", m.Body)
		return nil
	})
	if err := emailConsumer.Run(ctx); err != nil {
		log.Fatalf("run email consumer: %v", err)
	}

	// Consumer: SMS service
	smsCh := make(chan string, 5)
	smsConsumer := redissmq.NewConsumer()
	smsConsumer.ConsumeWithGroup(notifQueue, "sms-service", func(ctx context.Context, m *message.Transferable) error {
		smsCh <- fmt.Sprintf("SMS received: %v", m.Body)
		return nil
	})

	if err := smsConsumer.Run(ctx); err != nil {
		log.Fatalf("run sms consumer: %v", err)
	}

	// Produce one message
	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		log.Fatalf("run producer: %v", err)
	}

	m := message.New().
		SetBody("System maintenance at 2 AM").
		SetQueue(notifQueue)

	ids, err := p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	// Pub/Sub returns one ID per consumer group
	log.Printf("Produced: %v (expecting %d IDs)", ids, 2)

	// Both services should receive the message
	for i := 0; i < 2; i++ {
		select {
		case msg := <-emailCh:
			log.Println(msg)
		case msg := <-smsCh:
			log.Println(msg)
		case <-time.After(10 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	log.Println("Pub/Sub example complete")
}
