/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer_test

import (
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Produce a single message to a queue
func TestProduce_SingleMessage(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-single")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("hello").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
	if ids[0] == "" {
		t.Fatal("expected non-empty message ID")
	}
}

// Scenario: Produce multiple messages
func TestProduce_MultipleMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-multi")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	var allIDs []string
	for i := 0; i < 10; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		ids, err := prod.Produce(ctx, m)
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		allIDs = append(allIDs, ids[0])
	}

	if len(allIDs) != 10 {
		t.Fatalf("expected 10 IDs, got %d", len(allIDs))
	}

	// Verify queue has 10 messages
	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.MessagesCount != 10 {
		t.Fatalf("messages = %d, want 10", props.MessagesCount)
	}
	if props.PendingMessagesCount != 10 {
		t.Fatalf("pending = %d, want 10", props.PendingMessagesCount)
	}
}

// Scenario: Produce to non-existent queue
func TestProduce_NonExistentQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("nonexistent")
	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error for non-existent queue")
	}
}

// Scenario: Produce without starting producer
func TestProduce_NotRunning(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-not-running")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := redissmq.NewProducer()

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error when producer not running")
	}
}

// Scenario: Produce with TTL
func TestProduce_WithTTL(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-ttl")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("ttl-msg").
		SetQueue(params).
		SetTTL(5 * time.Minute).
		SetRetryThreshold(3).
		SetRetryDelay(10 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce scheduled message
func TestProduce_Scheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-scheduled")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Delayed message
	m := msg.New().
		SetBody("scheduled").
		SetQueue(params).
		SetScheduledDelay(1 * time.Hour)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce delayed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Verify it appears in scheduled, not pending
	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
	if props.PendingMessagesCount != 0 {
		t.Errorf("pending = %d, want 0", props.PendingMessagesCount)
	}
}

// Scenario: Produce to stopped queue fails
func TestProduce_StoppedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-stopped")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	queue.Stop(ctx, params, nil)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: cannot produce to stopped queue")
	}
}

// Scenario: Produce to paused queue succeeds
func TestProduce_PausedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-produce-paused")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	queue.Pause(ctx, params, nil)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce to paused queue: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Messages accumulate in pending
	props, _ := queue.Properties(ctx, params)
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
}

// Scenario: Producer ID is unique
func TestProduce_UniqueID(t *testing.T) {
	testutil.Setup(t)

	prod1 := redissmq.NewProducer()
	prod2 := redissmq.NewProducer()

	if prod1.ID() == prod2.ID() {
		t.Fatal("producer IDs should be unique")
	}
	if prod1.ID() == "" {
		t.Fatal("producer ID should not be empty")
	}
}

// Scenario: Producer run is idempotent
func TestProduce_RunTwice(t *testing.T) {
	ctx := testutil.Setup(t)

	prod := redissmq.NewProducer()

	err := prod.Run(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	err = prod.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	prod.Shutdown(ctx)
}

// Scenario: Shutdown is idempotent
func TestProduce_ShutdownIdempotent(t *testing.T) {
	ctx := testutil.Setup(t)

	prod := redissmq.NewProducer()
	prod.Run(ctx)

	prod.Shutdown(ctx)
	prod.Shutdown(ctx)
	prod.Shutdown(ctx)
}
