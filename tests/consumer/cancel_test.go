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

// Scenario: Cancel stops consuming from a queue
func TestCancel_StopConsuming(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-stop")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("before-cancel").SetQueue(params))

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Wait for initial consumption
	time.Sleep(2 * time.Second)
	beforeCancel := consumed.Load()
	t.Logf("consumed before cancel: %d", beforeCancel)

	// Cancel the queue
	cons.Cancel(params)
	time.Sleep(1 * time.Second)

	// Produce more messages — should not be consumed
	prod.Produce(ctx, msg.New().SetBody("after-cancel").SetQueue(params))
	time.Sleep(3 * time.Second)

	afterCancel := consumed.Load()
	t.Logf("consumed after cancel: %d", afterCancel)

	if afterCancel > beforeCancel {
		t.Fatal("messages consumed after cancel")
	}
}

// Scenario: Cancel with group stops consuming from a specific group
func TestCancel_StopConsumingWithGroup(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-group")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPubSub)

	// Create consumer group
	queue.SaveConsumerGroup(ctx, params, "workers")

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, "workers", func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)
	beforeCancel := consumed.Load()
	t.Logf("consumed before cancel: %d", beforeCancel)

	// Cancel the group
	cons.CancelWithGroup(params, "workers")
	time.Sleep(1 * time.Second)

	prod.Produce(ctx, msg.New().SetBody("after-cancel").SetQueue(params))
	time.Sleep(3 * time.Second)

	afterCancel := consumed.Load()
	t.Logf("consumed after cancel: %d", afterCancel)

	if afterCancel > beforeCancel {
		t.Fatal("messages consumed after cancel")
	}
}

// Scenario: Cancel is idempotent
func TestCancel_Idempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-idempotent")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(200 * time.Millisecond)

	cons.Cancel(params)
	cons.Cancel(params) // Second cancel should not panic
	cons.Cancel(params) // Third cancel should not panic
}

// Scenario: Consumer still running after canceling all queues
func TestCancel_ConsumerStillRunning(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-cancel-still-running")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(200 * time.Millisecond)

	cons.Cancel(params)

	time.Sleep(200 * time.Millisecond)

	if !cons.IsRunning() {
		t.Fatal("consumer should still be running after canceling all queues")
	}
}
