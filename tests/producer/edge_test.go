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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Produce with all message options set
func TestEdge_AllOptions(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-all-options")
	testutil.CreateQueue(t, ctx, params, q.TypePriority, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody(map[string]interface{}{"key": "value", "nested": map[string]int{"a": 1}}).
		SetQueue(params).
		SetTTL(10 * time.Minute).
		SetPriority(msg.PriorityHigh).
		SetRetryThreshold(5).
		SetRetryDelay(30 * time.Second).
		SetConsumeTimeout(60 * time.Second).
		SetScheduledCron("0 0 * * *").
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce with nil body
func TestEdge_NilBody(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-nil-body")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetQueue(params) // No body set
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce with nil body: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce with very long queue name
func TestEdge_LongQueueName(t *testing.T) {
	ctx := testutil.Setup(t)

	longName := "this-is-a-very-long-queue-name-that-pushes-the-limits-of-redis-key-length-but-should-still-be-valid"
	params := q.MustQueueParams(longName)
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce with unicode body
func TestEdge_UnicodeBody(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-unicode")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	unicodeBody := "Hello, 世界! 🚀 emoji and unicode: café, naïve, 中文, 한국어, العربية"
	m := msg.New().SetBody(unicodeBody).SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Produce with complex nested JSON body
func TestEdge_ComplexJSONBody(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-complex-json")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	complexBody := map[string]interface{}{
		"string": "value",
		"number": 42,
		"float":  3.14,
		"bool":   true,
		"null":   nil,
		"array":  []interface{}{1, "two", 3.0, true, nil},
		"nested": map[string]interface{}{
			"deep": map[string]interface{}{
				"deeper": "value",
			},
		},
	}

	m := msg.New().SetBody(complexBody).SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Rapid produce and shutdown cycles
func TestEdge_RapidProduceShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-rapid-prod")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	for i := 0; i < 10; i++ {
		prod := testutil.StartProducer(t, ctx)
		m := msg.New().SetBody("msg").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			t.Fatalf("cycle %d produce: %v", i, err)
		}
		prod.Shutdown(ctx)
	}
}

// Scenario: Produce with only exchange (no queue) should fail
func TestEdge_OnlyExchangeNoQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg") // No queue or exchange set
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: must have queue or exchange")
	}
}

// Scenario: Produce with priority on non-priority queue fails
func TestEdge_PriorityOnNonPriorityQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-prio-on-fifo")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("msg").SetQueue(params).SetPriority(msg.PriorityHigh)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: priority not supported on FIFO queue")
	}
}

// Scenario: Produce after shutdown fails
func TestEdge_ProduceAfterShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-edge-after-shutdown")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Shutdown(ctx)

	m := msg.New().SetBody("msg").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: producer is shut down")
	}
}
