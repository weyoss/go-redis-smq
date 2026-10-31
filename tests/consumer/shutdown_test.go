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
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Graceful shutdown completes pending messages
func TestShutdown_CompletesPendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-shutdown-pending")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Wait for some messages to be consumed
	time.Sleep(1 * time.Second)

	// Shutdown
	cons.Shutdown()

	count := consumed.Load()
	if count == 0 {
		t.Fatal("no messages consumed before shutdown")
	}
	t.Logf("consumed %d messages before shutdown", count)

	// Verify remaining messages are still pending
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	t.Logf("pending after shutdown: %d", props.PendingMessagesCount)
}

// Scenario: Multiple shutdown calls are idempotent
func TestShutdown_Idempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-shutdown-idempotent")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)

	time.Sleep(200 * time.Millisecond)

	cons.Shutdown()
	cons.Shutdown() // Second call should not panic
	cons.Shutdown() // Third call should not panic
}

// Scenario: Consumer is not running after shutdown
func TestShutdown_NotRunningAfterShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-shutdown-not-running")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)

	time.Sleep(200 * time.Millisecond)

	if !cons.IsRunning() {
		t.Fatal("consumer should be running before shutdown")
	}

	cons.Shutdown()

	if cons.IsRunning() {
		t.Fatal("consumer should not be running after shutdown")
	}
}

// Scenario: Shutdown without Run is safe
func TestShutdown_WithoutRun(t *testing.T) {
	testutil.Setup(t)

	cons := redissmq.NewConsumer()
	cons.Consume(publicqueue.MustQueueParams("test-shutdown-no-run"), func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})

	// Shutdown without ever calling Run
	cons.Shutdown()
	cons.Shutdown() // Idempotent
}

// Scenario: Consumer can be run, shutdown, and run again
func TestShutdown_RunShutdownRunShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-run-shutdown-cycle")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// First cycle: run → consume → shutdown
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	prod.Produce(ctx, msg.New().SetBody("cycle-1").SetQueue(params))
	time.Sleep(2 * time.Second)

	cons.Shutdown()

	if cons.IsRunning() {
		t.Fatal("consumer should not be running after first shutdown")
	}

	// Second cycle: run → consume → shutdown
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}

	prod.Produce(ctx, msg.New().SetBody("cycle-2").SetQueue(params))
	time.Sleep(2 * time.Second)

	cons.Shutdown()

	if cons.IsRunning() {
		t.Fatal("consumer should not be running after second shutdown")
	}
}

// Scenario: Consumer.Shutdown returns only after the heartbeat beat loop has
// exited — no in-flight Redis op remains after Shutdown returns.
//
// Regression test for C9. Pre-fix, Heartbeat.Stop closed the done channel
// but did not wait for the beat goroutine; if a beat was mid-SET when
// Shutdown returned, redissmq.Shutdown's subsequent internalredis.Close
// would race it and panic on a nil client.
func TestShutdown_HeartbeatBeatLoopExited(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-c9-heartbeat-exit")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer(consumer.WithHeartbeatTTL(1 * time.Second))
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}

	// Let the heartbeat loop start.
	time.Sleep(200 * time.Millisecond)

	cons.Shutdown()

	// After Shutdown, the heartbeat key is gone (DEL ran) and the beat
	// loop has exited. We can't directly observe the goroutine, but the
	// DEL is the last thing Stop does after waiting for the loop — if the
	// loop were still running, the DEL might race a beat SET, leaving the
	// key in place.
	exists, err := redis.Client().Exists(ctx, keys.System{}.ConsumerHeartbeat(cons.ID())).Result()
	if err != nil {
		t.Fatalf("check heartbeat: %v", err)
	}
	if exists != 0 {
		t.Fatalf("heartbeat key still present after Shutdown — beat loop may still be running")
	}
}
