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
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	queue2 "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Full message lifecycle — produce → consume → ack → get → requeue → consume
func TestComplex_FullLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := queue2.MustQueueParams("test-complex-lifecycle")
	testutil.CreateQueue(t, ctx, params, queue2.TypeFIFO, queue2.DeliveryPointToPoint)

	// Produce
	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().SetBody("lifecycle").SetQueue(params))
	originalID := ids[0]
	t.Logf("produced: %s", originalID)

	// Verify status is pending
	status, _ := message.Status(ctx, originalID)
	if status != msg.StatusPending {
		t.Errorf("status after produce = %s, want PENDING", status.String())
	}

	// Consume and acknowledge
	received := make(chan string, 1)
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received <- m.ID
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	select {
	case id := <-received:
		if id != originalID {
			t.Errorf("received %s, want %s", id, originalID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	time.Sleep(200 * time.Millisecond)

	// Verify message still exists (can be retrieved)
	m, err := message.Get(ctx, originalID)
	if err != nil {
		t.Fatalf("get after ack: %v", err)
	}
	t.Logf("message status after ack: %s", m.Status.String())

	// Requeue
	newID, err := message.Requeue(ctx, originalID)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	t.Logf("requeued: %s -> %s", originalID, newID)

	// Verify new message can be consumed
	var newConsumed atomic.Int64
	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
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

// Scenario: High volume produce and delete
func TestComplex_HighVolumeDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue2.MustQueueParams("test-complex-high-vol-del")
	testutil.CreateQueue(t, ctx, params, queue2.TypeFIFO, queue2.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	count := 50
	var ids []string
	for i := 0; i < count; i++ {
		id, _ := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
		ids = append(ids, id[0])
	}

	// Delete all
	result, err := message.DeleteAll(ctx, ids)
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if result.Stats.Success != count {
		t.Errorf("success = %d, want %d", result.Stats.Success, count)
	}
	t.Logf("deleted %d/%d messages", result.Stats.Success, count)
}

// Scenario: Message state transitions
func TestComplex_StateTransitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := queue2.MustQueueParams("test-complex-states")
	testutil.CreateQueue(t, ctx, params, queue2.TypeFIFO, queue2.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce immediate message
	ids, _ := prod.Produce(ctx, msg.New().SetBody("state-test").SetQueue(params))
	id := ids[0]

	// Check initial state
	state, _ := message.State(ctx, id)
	if state.Attempts != 0 {
		t.Errorf("initial attempts = %d, want 0", state.Attempts)
	}

	// Consume and acknowledge
	received := make(chan struct{})
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		received <- struct{}{}
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()
	<-received
	time.Sleep(200 * time.Millisecond)

	// Check state after ack
	state, _ = message.State(ctx, id)
	t.Logf("state after ack: attempts=%d, expired=%v", state.Attempts, state.Expired)
}

// Scenario: Multiple queues with browsing
func TestComplex_MultiQueueBrowse(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := queue2.MustQueueParams("test-complex-browse-q1")
	q2 := queue2.MustQueueParams("test-complex-browse-q2")
	testutil.CreateQueue(t, ctx, q1, queue2.TypeFIFO, queue2.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue2.TypeFIFO, queue2.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce to both queues
	for i := 0; i < 3; i++ {
		prod.Produce(ctx, msg.New().SetBody("q1").SetQueue(q1))
	}
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("q2").SetQueue(q2))
	}

	qm := redissmq.NewQueueManager()

	// Browse each queue independently
	r1, _ := qm.BrowseMessages(ctx, q1, &queue2.BrowseParams{Filter: queue2.BrowsePublished})
	r2, _ := qm.BrowseMessages(ctx, q2, &queue2.BrowseParams{Filter: queue2.BrowsePublished})

	if r1.Total != 3 {
		t.Errorf("q1 total = %d, want 3", r1.Total)
	}
	if r2.Total != 5 {
		t.Errorf("q2 total = %d, want 5", r2.Total)
	}
}

// Scenario: Scheduled message lifecycle
func TestComplex_ScheduledLifecycle(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue2.MustQueueParams("test-complex-sched-life")
	testutil.CreateQueue(t, ctx, params, queue2.TypeFIFO, queue2.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce scheduled message
	ids, _ := prod.Produce(ctx, msg.New().
		SetBody("scheduled-life").
		SetQueue(params).
		SetScheduledDelay(1*time.Hour),
	)
	id := ids[0]

	// Check status is scheduled
	status, _ := message.Status(ctx, id)
	if status != msg.StatusScheduled {
		t.Errorf("status = %s, want SCHEDULED", status.String())
	}

	// Get the message — should be retrievable while scheduled
	m, err := message.Get(ctx, id)
	if err != nil {
		t.Fatalf("get scheduled: %v", err)
	}
	if m.Status != msg.StatusScheduled {
		t.Errorf("message status = %s, want SCHEDULED", m.Status.String())
	}

	// Delete the scheduled message
	result, err := message.Delete(ctx, id)
	if err != nil {
		t.Fatalf("delete scheduled: %v", err)
	}
	if result.Stats.Success != 1 {
		t.Errorf("delete success = %d, want 1", result.Stats.Success)
	}

	// Verify it's gone
	_, err = message.Get(ctx, id)
	if err == nil {
		t.Fatal("message should be deleted")
	}
}

// Scenario: Message with all properties
func TestComplex_AllProperties(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue2.MustQueueParams("test-complex-all-props")
	testutil.CreateQueue(t, ctx, params, queue2.TypePriority, queue2.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	complexBody := map[string]interface{}{
		"orderId":  "ORD-12345",
		"items":    []string{"item1", "item2"},
		"total":    99.99,
		"currency": "USD",
	}

	ids, err := prod.Produce(ctx, msg.New().
		SetBody(complexBody).
		SetQueue(params).
		SetPriority(msg.PriorityHigh).
		SetTTL(30*time.Minute).
		SetRetryThreshold(5).
		SetRetryDelay(15*time.Second).
		SetConsumeTimeout(60*time.Second),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	m, err := message.Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Verify all properties
	if m.TTL != 30*60*1000 {
		t.Errorf("ttl = %d, want %d", m.TTL, 30*60*1000)
	}
	if m.RetryThreshold != 5 {
		t.Errorf("retry threshold = %d, want 5", m.RetryThreshold)
	}
	if m.RetryDelay != 15000 {
		t.Errorf("retry delay = %d, want 15000", m.RetryDelay)
	}
	if m.ConsumeTimeout != 60000 {
		t.Errorf("consume timeout = %d, want 60000", m.ConsumeTimeout)
	}
	if m.Priority == nil || *m.Priority != msg.PriorityHigh {
		t.Error("priority not set correctly")
	}
	t.Logf("message ID: %s", m.ID)
	t.Logf("created at: %d", m.CreatedAt)
}
