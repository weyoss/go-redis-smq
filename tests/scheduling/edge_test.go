/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package scheduling_test

import (
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Schedule message with all options set
func TestEdge_AllOptions(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-all-options")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("all-options").
		SetQueue(params).
		SetScheduledDelay(10 * time.Second).
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

// Scenario: Reset params after setting
func TestEdge_ResetAfterSet(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-reset-after")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("reset-after").SetQueue(params)
	m.SetScheduledDelay(10 * time.Second)
	m.ResetScheduledParams()
	m.SetScheduledCron("0 0 * * *")

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Maximum repeat count
func TestEdge_MaxRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-max-repeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("max-repeat").
		SetQueue(params).
		SetScheduledRepeat(999999).
		SetScheduledRepeatPeriod(1 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Very short delay
func TestEdge_VeryShortDelay(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-short-delay")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("short-delay").
		SetQueue(params).
		SetScheduledDelay(1 * time.Millisecond)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Very long delay
func TestEdge_VeryLongDelay(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-long-delay")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("long-delay").
		SetQueue(params).
		SetScheduledDelay(87600 * time.Hour) // 10 years

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be in scheduled
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: Multiple delays on same message (last one wins)
func TestEdge_MultipleDelaysLastWins(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-multi-delays")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("multi-delays").
		SetQueue(params).
		SetScheduledDelay(1 * time.Hour).
		SetScheduledDelay(30 * time.Minute) // Last one wins

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Scheduled message on priority queue
func TestEdge_ScheduledOnPriorityQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-sched-prio")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypePriority, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("sched-prio").
		SetQueue(params).
		SetPriority(msg.PriorityHigh).
		SetScheduledDelay(1 * time.Hour)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: Rapid schedule and delete cycles
func TestEdge_RapidScheduleDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-rapid-sched-del")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	for i := 0; i < 10; i++ {
		m := msg.New().
			SetBody("rapid").
			SetQueue(params).
			SetScheduledDelay(1 * time.Hour)

		ids, err := prod.Produce(ctx, m)
		if err != nil {
			t.Fatalf("cycle %d produce: %v", i, err)
		}

		// Delete it immediately
		result, err := redissmq.NewMessageManager().Delete(ctx, ids[0])
		if err != nil {
			t.Fatalf("cycle %d delete: %v", i, err)
		}
		if result.Status != msg.DeleteStatusOK {
			t.Errorf("cycle %d: status = %s, want OK", i, result.Status)
		}
	}
}

// Scenario: Schedule then disable priority
func TestEdge_ScheduleThenDisablePriority(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-edge-disable-prio")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypePriority, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("disable-prio").
		SetQueue(params).
		SetPriority(msg.PriorityHigh).
		SetScheduledDelay(1 * time.Hour)

	// Disable priority before producing
	m.DisablePriority()

	// Priority queue requires priority — should fail
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected error: priority required for priority queue")
	}
}
