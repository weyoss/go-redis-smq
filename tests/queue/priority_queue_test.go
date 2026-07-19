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
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Create a priority queue
func TestPriorityQueue_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-create")
	err := queue.Create(ctx, params, q.TypePriority, q.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.Type != q.TypePriority {
		t.Fatalf("type = %v, want PRIORITY", props.Type)
	}
}

// Scenario: Produce to priority queue without priority returns error
func TestPriorityQueue_ProduceWithoutPriority(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-no-prio")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	// No priority set — should fail
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: priority required for priority queue")
	}
}

// Scenario: Priority queue delivers higher priority messages first
func TestPriorityQueue_PriorityOrder(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-order")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Produce messages with different priorities
	// Lower number = higher priority
	prod.Produce(ctx, msg.New().SetBody("low").SetQueue(params).SetPriority(msg.PriorityLow))         // 5
	prod.Produce(ctx, msg.New().SetBody("normal").SetQueue(params).SetPriority(msg.PriorityNormal))   // 4
	prod.Produce(ctx, msg.New().SetBody("high").SetQueue(params).SetPriority(msg.PriorityHigh))       // 2
	prod.Produce(ctx, msg.New().SetBody("highest").SetQueue(params).SetPriority(msg.PriorityHighest)) // 0

	// Consume and record order
	var order []string
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		order = append(order, m.Body.(string))
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	if len(order) != 4 {
		t.Fatalf("consumed %d messages, want 4", len(order))
	}

	// Highest priority (0) should be first, lowest (5) last
	expectedOrder := []string{"highest", "high", "normal", "low"}
	for i, expected := range expectedOrder {
		if order[i] != expected {
			t.Errorf("position %d: got %s, want %s", i, order[i], expected)
		}
	}
	t.Logf("consumption order: %v", order)
}

// Scenario: Messages with same priority are consumed
// Same-priority messages have no guaranteed order — just verify they're all consumed
func TestPriorityQueue_SamePriority(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-same")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	prod.Produce(ctx, msg.New().SetBody("first").SetQueue(params).SetPriority(msg.PriorityNormal))
	prod.Produce(ctx, msg.New().SetBody("second").SetQueue(params).SetPriority(msg.PriorityNormal))
	prod.Produce(ctx, msg.New().SetBody("third").SetQueue(params).SetPriority(msg.PriorityNormal))

	var count atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		count.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(2 * time.Second)

	if count.Load() != 3 {
		t.Fatalf("consumed %d messages, want 3", count.Load())
	}
}

// Scenario: Browse pending messages in priority queue (sorted set)
func TestPriorityQueue_BrowsePending(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-browse")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg1").SetQueue(params).SetPriority(msg.PriorityNormal))
	prod.Produce(ctx, msg.New().SetBody("msg2").SetQueue(params).SetPriority(msg.PriorityHigh))

	result, err := queue.BrowseMessages(ctx, params, &q.BrowseParams{
		Filter: q.BrowsePending,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 2 {
		t.Fatalf("total = %d, want 2", result.Total)
	}
	// Priority queue uses sorted set — results ordered by priority (highest first)
}

// Scenario: Disable priority on a message
func TestPriorityQueue_DisablePriority(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-disable")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params).SetPriority(msg.PriorityHigh)
	m.DisablePriority()

	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: priority required for priority queue after disabling")
	}
}

// Scenario: Produce with priority to non-priority queue returns error
func TestPriorityQueue_PriorityOnFIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-priority-on-fifo")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params).SetPriority(msg.PriorityHigh)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: priority not supported on FIFO queue")
	}
}
