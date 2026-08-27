/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Example: Fanout exchange — broadcast to all bound queues.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	if err := redissmq.Init(ctx, rdb); err != nil {
		log.Fatalf("init: %v", err)
	}
	defer redissmq.Shutdown()

	//
	qm := redissmq.NewQueueManager()

	// Create queues
	emailQueue := queue.MustQueueParams(fmt.Sprintf("email-alerts-%d", time.Now().UnixMilli()))
	qm.Create(ctx, emailQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	smsQueue := queue.MustQueueParams(fmt.Sprintf("sms-alerts-%d", time.Now().UnixMilli()))
	qm.Create(ctx, smsQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	pushQueue := queue.MustQueueParams(fmt.Sprintf("push-alerts-%d", time.Now().UnixMilli()))
	qm.Create(ctx, pushQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Create fanout exchange
	fx := redissmq.NewFanoutExchange()
	exchangeParams := exchange.MustExchangeParams("system-alerts", exchange.TypeFanout)

	// Bind all queues — no routing key needed
	if err := fx.BindQueue(ctx, emailQueue, exchangeParams); err != nil {
		log.Fatalf("bind email: %v", err)
	}
	if err := fx.BindQueue(ctx, smsQueue, exchangeParams); err != nil {
		log.Fatalf("bind sms: %v", err)
	}
	if err := fx.BindQueue(ctx, pushQueue, exchangeParams); err != nil {
		log.Fatalf("bind push: %v", err)
	}

	// Show bound queues
	bound, _ := fx.BoundQueues(ctx, exchangeParams)
	log.Printf("Bound queues: %d", len(bound))
	for _, q := range bound {
		log.Printf("  - %s", q.String())
	}

	// Consumers
	type result struct {
		queue string
		body  interface{}
	}
	ch := make(chan result, 3)

	startConsumer := func(queue *queue.Params, label string) {
		c := redissmq.NewConsumer()
		c.Consume(queue, func(ctx context.Context, m *message.Transferable) error {
			ch <- result{queue: label, body: m.Body}
			return nil
		})
		c.Run(ctx)
	}

	startConsumer(emailQueue, "email")
	startConsumer(smsQueue, "sms")
	startConsumer(pushQueue, "push")

	// Producer
	p := redissmq.NewProducer()
	p.Run(ctx)

	// Send one message — goes to ALL bound queues
	m := message.New().
		SetBody(map[string]interface{}{
			"alert":   "System maintenance in 5 minutes",
			"urgency": "high",
		}).
		SetFanoutExchange(exchangeParams)
	// No routing key needed for fanout

	ids, err := p.Produce(ctx, m)
	if err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("Produced 1 message → %d queue(s): %v", len(ids), ids)

	// All 3 queues should receive the message
	for i := 0; i < 3; i++ {
		select {
		case r := <-ch:
			log.Printf("[%s] Received: %v", r.queue, r.body)
		case <-time.After(10 * time.Second):
			log.Fatal("timeout waiting for messages")
		}
	}

	log.Println("Fanout exchange example complete")
}
