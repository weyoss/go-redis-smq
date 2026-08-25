/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Requeue an acknowledged message
func TestRequeue_AcknowledgedMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-requeue-acked")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("requeue-me").SetQueue(params))

	// Consume to acknowledge
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *message.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()
	<-received
	time.Sleep(200 * time.Millisecond)

	// Requeue the acknowledged message
	newID, err := redissmq.NewMessageManager().Requeue(ctx, ids[0])
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if newID == "" {
		t.Fatal("expected new message ID")
	}
	if newID == ids[0] {
		t.Fatal("requeued message should have a new ID")
	}

	// Verify new message can be consumed
	var newConsumed atomic.Int64
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *message.Transferable) error {
		newConsumed.Add(1)
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	time.Sleep(2 * time.Second)
	if newConsumed.Load() != 1 {
		t.Errorf("requeued message not consumed")
	}
}

// Scenario: Requeue a scheduled message fails
func TestRequeue_RequeueScheduledMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-edge-req-sched")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().
		SetBody("sched-req").
		SetQueue(params).
		SetScheduledDelay(1*time.Hour),
	)

	_, err := redissmq.NewMessageManager().Requeue(ctx, ids[0])
	if err == nil {
		t.Fatal("expected error: cannot requeue scheduled message")
	}
}

// Scenario: Requeue a pending message fails
func TestRequeue_PendingMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-requeue-pending")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("pending").SetQueue(params))

	// Try to requeue a pending message — should fail
	_, err := redissmq.NewMessageManager().Requeue(ctx, ids[0])
	if err == nil {
		t.Fatal("expected error: cannot requeue pending message")
	}
}

// Scenario: Requeue a non-existent message fails
func TestRequeue_NotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	_, err := redissmq.NewMessageManager().Requeue(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

// Scenario: Requeue preserves body and queue destination
func TestRequeue_PreservesMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-requeue-preserve")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	originalBody := map[string]interface{}{"orderId": 123, "amount": 99.99}
	ids, _ := prod.Produce(ctx, message.New().
		SetBody(originalBody).
		SetQueue(params).
		SetTTL(5*time.Minute),
	)

	// Consume to acknowledge
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *message.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()
	<-received
	time.Sleep(200 * time.Millisecond)

	mm := redissmq.NewMessageManager()

	// Requeue
	newID, err := mm.Requeue(ctx, ids[0])
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}

	// Get the new message
	newMsg, err := mm.Get(ctx, newID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Body should be preserved
	if newMsg.Body == nil {
		t.Fatal("body should not be nil")
	}

	// Destination queue should be preserved
	if newMsg.DestinationQueue == nil {
		t.Fatal("destination queue should be set")
	}
	if newMsg.DestinationQueue.Name() != params.Name() {
		t.Errorf("queue name = %s, want %s", newMsg.DestinationQueue.Name(), params.Name())
	}

	// TTL should be preserved
	if newMsg.TTL != 5*60*1000 {
		t.Errorf("ttl = %d, want %d", newMsg.TTL, 5*60*1000)
	}
}

// Scenario: Requeue same message twice creates two new messages
func TestRequeue_DoubleRequeue(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := queue.MustQueueParams("test-requeue-double")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().SetBody("double-requeue").SetQueue(params))

	// Consume to acknowledge
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *message.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()
	<-received
	time.Sleep(200 * time.Millisecond)

	mm := redissmq.NewMessageManager()

	// Requeue twice
	newID1, err := mm.Requeue(ctx, ids[0])
	if err != nil {
		t.Fatalf("first requeue: %v", err)
	}
	newID2, err := mm.Requeue(ctx, ids[0])
	if err != nil {
		t.Fatalf("second requeue: %v", err)
	}

	if newID1 == newID2 {
		t.Fatal("requeue should create unique IDs each time")
	}
	if newID1 == "" || newID2 == "" {
		t.Fatal("requeue IDs should not be empty")
	}

	t.Logf("original: %s, requeue 1: %s, requeue 2: %s", ids[0], newID1, newID2)
}

// Scenario: Requeue from dead-lettered message
func TestRequeue_DeadLetteredMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	// Enable dead-letter audit
	cfg := config.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	config.Save(ctx, cfg)

	params := queue.MustQueueParams("test-requeue-dlq")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, message.New().
		SetBody("dlq-requeue").
		SetQueue(params).
		SetRetryThreshold(1). // Fail once, then DLQ
		SetRetryDelay(0),
	)

	// Consume and fail to trigger DLQ
	var attempts atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *message.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("fail")
	})
	cons.Run(ctx)

	time.Sleep(8 * time.Second)
	cons.Shutdown()

	t.Logf("attempts: %d", attempts.Load())

	// Requeue from DLQ
	newID, err := redissmq.NewMessageManager().Requeue(ctx, ids[0])
	if err != nil {
		t.Fatalf("requeue from DLQ: %v", err)
	}
	if newID == "" {
		t.Fatal("expected new message ID from DLQ requeue")
	}
	t.Logf("requeued DLQ message: %s -> %s", ids[0], newID)
}
