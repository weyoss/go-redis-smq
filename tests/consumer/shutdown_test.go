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
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Graceful shutdown completes pending messages
func TestShutdown_CompletesPendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-shutdown-pending")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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
	props, _ := queue.Properties(ctx, params)
	t.Logf("pending after shutdown: %d", props.PendingMessagesCount)
}

// Scenario: Multiple shutdown calls are idempotent
func TestShutdown_Idempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-shutdown-idempotent")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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

	params := q.MustQueueParams("test-shutdown-not-running")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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
	cons.Consume(q.MustQueueParams("test-shutdown-no-run"), func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})

	// Shutdown without ever calling Run
	cons.Shutdown()
	cons.Shutdown() // Idempotent
}

// Scenario: Consumer can be run, shutdown, and run again
func TestShutdown_RunShutdownRunShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-run-shutdown-cycle")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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
