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
)

// Scenario: Add handler to a running consumer
func TestDynamic_AddHandlerAfterRun(t *testing.T) {
	ctx := testutil.Setup(t)

	params1 := queue.MustQueueParams("test-dynamic-add-1")
	params2 := queue.MustQueueParams("test-dynamic-add-2")
	testutil.CreateQueue(t, ctx, params1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, params2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg1").SetQueue(params1))
	prod.Produce(ctx, msg.New().SetBody("msg2").SetQueue(params2))

	var consumed1, consumed2 atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(params1, func(ctx context.Context, m *msg.Transferable) error {
		consumed1.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Wait for first queue to be consumed
	time.Sleep(2 * time.Second)
	t.Logf("consumed from queue1: %d", consumed1.Load())

	// Add second handler while running
	cons.Consume(params2, func(ctx context.Context, m *msg.Transferable) error {
		consumed2.Add(1)
		return nil
	})

	// Wait for second queue to be consumed
	time.Sleep(3 * time.Second)

	t.Logf("consumed from queue1: %d", consumed1.Load())
	t.Logf("consumed from queue2: %d", consumed2.Load())

	if consumed1.Load() == 0 {
		t.Fatal("no messages consumed from queue1")
	}
	if consumed2.Load() == 0 {
		t.Fatal("no messages consumed from queue2 after adding handler")
	}
}

// Scenario: Replace handler on a running consumer
func TestDynamic_ReplaceHandler(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-dynamic-replace")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var handledBy1, handledBy2 atomic.Int64

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handledBy1.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	time.Sleep(2 * time.Second)
	t.Logf("handled by first handler: %d", handledBy1.Load())

	// Replace with new handler
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		handledBy2.Add(1)
		return nil
	})

	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	time.Sleep(3 * time.Second)

	t.Logf("handled by first handler: %d", handledBy1.Load())
	t.Logf("handled by second handler: %d", handledBy2.Load())

	if handledBy2.Load() == 0 {
		t.Fatal("new handler did not receive messages")
	}
}

// Scenario: Remove handler from a running consumer
func TestDynamic_RemoveHandler(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-dynamic-remove")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod.Produce(ctx, msg.New().SetBody("before-remove").SetQueue(params))
	time.Sleep(2 * time.Second)
	beforeRemove := consumed.Load()
	t.Logf("consumed before remove: %d", beforeRemove)

	// Remove the handler
	cons.Cancel(params)
	time.Sleep(1 * time.Second)

	prod.Produce(ctx, msg.New().SetBody("after-remove").SetQueue(params))
	time.Sleep(3 * time.Second)

	afterRemove := consumed.Load()
	t.Logf("consumed after remove: %d", afterRemove)

	if afterRemove > beforeRemove {
		t.Fatal("messages consumed after handler removed")
	}
}

// Scenario: Consumer with no handlers stays running
func TestDynamic_NoHandlersStaysRunning(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-dynamic-no-handlers")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(200 * time.Millisecond)

	// Remove all handlers
	cons.Cancel(params)

	time.Sleep(500 * time.Millisecond)

	if !cons.IsRunning() {
		t.Fatal("consumer should stay running even with no handlers")
	}
}
