/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Consumer heartbeat key exists while running
func TestHeartbeat_Running(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-hb-running")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer(
		consumer.WithHeartbeatTTL(10 * time.Second),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(500 * time.Millisecond)

	exists, err := redis.Client().Exists(ctx, keys.System{}.ConsumerHeartbeat(cons.ID())).Result()
	if err != nil {
		t.Fatalf("check heartbeat: %v", err)
	}
	if exists == 0 {
		t.Fatal("heartbeat key should exist while consumer is running")
	}
}

// Scenario: Consumer heartbeat key expires after shutdown
func TestHeartbeat_ExpiresAfterShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-hb-shutdown")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer(
		consumer.WithHeartbeatTTL(1 * time.Second),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	cons.Shutdown()

	time.Sleep(2 * time.Second)

	exists, err := redis.Client().Exists(ctx, keys.System{}.ConsumerHeartbeat(cons.ID())).Result()
	if err != nil {
		t.Fatalf("check heartbeat: %v", err)
	}
	if exists > 0 {
		t.Fatal("heartbeat key should expire after shutdown")
	}
}

// Scenario: Consumer heartbeat TTL is refreshed periodically
func TestHeartbeat_TTLRefresh(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-hb-refresh")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer(
		consumer.WithHeartbeatTTL(3 * time.Second),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(4 * time.Second)

	exists, err := redis.Client().Exists(ctx, keys.System{}.ConsumerHeartbeat(cons.ID())).Result()
	if err != nil {
		t.Fatalf("check heartbeat: %v", err)
	}
	if exists == 0 {
		t.Fatal("heartbeat key should still exist (TTL refreshed)")
	}
}
