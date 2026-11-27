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

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
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

	emailQueue := queue.MustQueueParams(fmt.Sprintf("email-alerts-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, emailQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create email queue: %w", err)
	}

	smsQueue := queue.MustQueueParams(fmt.Sprintf("sms-alerts-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, smsQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create sms queue: %w", err)
	}

	pushQueue := queue.MustQueueParams(fmt.Sprintf("push-alerts-%d", time.Now().UnixMilli()))
	if err := qm.Create(ctx, pushQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
		return fmt.Errorf("create push queue: %w", err)
	}

	fx := redissmq.NewFanoutExchange()
	exchangeParams := exchange.MustExchangeParams("system-alerts", exchange.TypeFanout)

	if err := fx.BindQueue(ctx, emailQueue, exchangeParams); err != nil {
		return fmt.Errorf("bind email: %w", err)
	}
	if err := fx.BindQueue(ctx, smsQueue, exchangeParams); err != nil {
		return fmt.Errorf("bind sms: %w", err)
	}
	if err := fx.BindQueue(ctx, pushQueue, exchangeParams); err != nil {
		return fmt.Errorf("bind push: %w", err)
	}

	bound, err := fx.BoundQueues(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("bound queues: %w", err)
	}
	log.Printf("Bound queues: %d", len(bound))
	for _, q := range bound {
		log.Printf("  - %s", q.String())
	}

	type result struct {
		queue string
		body  interface{}
	}
	ch := make(chan result, 3)

	startConsumer := func(q *queue.Params, label string) error {
		c := redissmq.NewConsumer()
		c.Consume(q, func(_ context.Context, m *message.Transferable) error {
			ch <- result{queue: label, body: m.Body}
			return nil
		})
		if err := c.Run(ctx); err != nil {
			return fmt.Errorf("run %s consumer: %w", label, err)
		}
		return nil
	}

	if err := startConsumer(emailQueue, "email"); err != nil {
		return err
	}
	if err := startConsumer(smsQueue, "sms"); err != nil {
		return err
	}
	if err := startConsumer(pushQueue, "push"); err != nil {
		return err
	}

	p := redissmq.NewProducer()
	if err := p.Run(ctx); err != nil {
		return fmt.Errorf("run producer: %w", err)
	}

	m := message.New().
		SetBody(map[string]interface{}{
			"alert":   "System maintenance in 5 minutes",
			"urgency": "high",
		}).
		SetFanoutExchange(exchangeParams)

	ids, err := p.Produce(ctx, m)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	log.Printf("Produced 1 message → %d queue(s): %v", len(ids), ids)

	for i := 0; i < 3; i++ {
		select {
		case r := <-ch:
			log.Printf("[%s] Received: %v", r.queue, r.body)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timeout waiting for messages")
		}
	}

	log.Println("Fanout exchange example complete")
	return nil
}
