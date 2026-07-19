/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Delete queue with pending messages returns error
func TestDeleteQueue_WithPendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete-pending")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Produce a message (no consumer, stays pending)
	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	err := queue.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with pending messages")
	}
}

// Scenario: Delete queue with active consumers returns error
func TestDeleteQueue_WithActiveConsumers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete-active-consumers")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Start a consumer
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(200 * time.Millisecond)

	err := queue.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with active consumers")
	}
}

// Scenario: Delete queue with bound exchange returns error
func TestDeleteQueue_WithBoundExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete-exchange")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Create and bind an exchange
	exchangeParams := x.MustExchangeParams("test-exchange", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.BindQueue(ctx, params, exchangeParams, "test.key")

	err := queue.Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with bound exchange")
	}
}

// Scenario: Delete queue succeeds when empty with no consumers
func TestDeleteQueue_EmptyQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete-empty")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	err := queue.Delete(ctx, params)
	if err != nil {
		t.Fatalf("delete empty queue: %v", err)
	}

	exists, _ := queue.Exists(ctx, params)
	if exists {
		t.Fatal("queue should not exist after delete")
	}
}

// Scenario: Delete queue after stopping consumers
func TestDeleteQueue_AfterConsumerShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delete-after-shutdown")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Start and stop a consumer
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	cons.Shutdown()
	time.Sleep(200 * time.Millisecond)

	// Now delete should succeed
	err := queue.Delete(ctx, params)
	if err != nil {
		t.Fatalf("delete after consumer shutdown: %v", err)
	}
}
