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
	"errors"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Delete queue with pending messages returns error
func TestDeleteQueue_WithPendingMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-pending")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Produce a message (no consumer, stays pending)
	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	err := redissmq.NewQueueManager().Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with pending messages")
	}
}

// Scenario: Delete queue with active consumers returns error
func TestDeleteQueue_WithActiveConsumers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-active-consumers")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Start a consumer
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(200 * time.Millisecond)

	err := redissmq.NewQueueManager().Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with active consumers")
	}
}

// Scenario: Delete queue with bound exchange returns error
func TestDeleteQueue_WithBoundExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-exchange")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Create and bind an exchange
	exchangeParams := exchange.MustExchangeParams("test-exchange", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.BindQueue(ctx, params, exchangeParams, "test.key")

	err := redissmq.NewQueueManager().Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error: cannot delete queue with bound exchange")
	}
}

// Scenario: Delete queue succeeds when empty with no consumers
func TestDeleteQueue_EmptyQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-empty")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	err := qm.Delete(ctx, params)
	if err != nil {
		t.Fatalf("delete empty queue: %v", err)
	}

	exists, _ := qm.Exists(ctx, params)
	if exists {
		t.Fatal("queue should not exist after delete")
	}
}

// Scenario: Delete queue after consumer shutdown (empty hash should succeed)
func TestDeleteQueue_AfterConsumerShutdown_EmptyHashSucceeds(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-after-shutdown-empty-hash")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Start and stop a consumer
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	cons.Shutdown()
	time.Sleep(200 * time.Millisecond)

	// Now delete should succeed
	if err := redissmq.NewQueueManager().Delete(ctx, params); err != nil {
		t.Fatalf("delete after consumer shutdown: %v", err)
	}
}

// Scenario: Delete queue with active consumer fails with ErrQueueHasActiveConsumers
func TestDeleteQueue_WithActiveConsumerFails(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-delete-with-active-consumer")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	// Give the consumer time to subscribe and set its heartbeat.
	time.Sleep(500 * time.Millisecond)

	err := redissmq.NewQueueManager().Delete(ctx, params)
	if err == nil {
		t.Fatal("expected error when deleting a queue with an active consumer")
	}
	if !errors.Is(err, publicqueue.ErrQueueHasActiveConsumers) {
		t.Fatalf("expected ErrQueueHasActiveConsumers, got: %v", err)
	}
}
