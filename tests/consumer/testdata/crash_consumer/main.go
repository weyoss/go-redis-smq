/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
	addr := os.Getenv("redissmq.ADDR")
	queueName := os.Getenv("QUEUE_NAME")
	queueNS := os.Getenv("QUEUE_NS")
	if addr == "" || queueName == "" {
		log.Fatal("missing required env vars: redissmq.ADDR, QUEUE_NAME, QUEUE_NS")
	}

	// Allow overriding the heartbeat TTL (default 2s for fast crash detection).
	hbTTL := 2 * time.Second
	if s := os.Getenv("HEARTBEAT_TTL"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			hbTTL = d
		}
	}

	ctx := context.Background()
	if err := redissmq.Init(ctx, redis.Options{Addr: addr}); err != nil {
		log.Fatal(err)
	}
	defer redissmq.Shutdown()

	params := queue.MustQueueParamsWithNS(queueName, queueNS)

	cons := redissmq.NewConsumer(consumer.WithHeartbeatTTL(hbTTL))
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		log.Printf("crash consumer: received %s", m.ID)
		// Block until externally killed to simulate a crash.
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		<-ch
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		log.Fatal(err)
	}

	// Keep running until killed.
	<-ctx.Done()
}
