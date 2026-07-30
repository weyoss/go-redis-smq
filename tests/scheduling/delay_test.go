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

// Scenario: Schedule a message with delay — appears in scheduled
func TestDelay_MessageInScheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delay-scheduled")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("delayed").
		SetQueue(params).
		SetScheduledDelay(1 * time.Hour)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

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

// Scenario: Short delay — message is consumed after delay
func TestDelay_ShortDelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-delay-short")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("short-delay").
		SetQueue(params).
		SetScheduledDelay(2 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	t.Logf("produced: %v", ids)

	// Verify in scheduled initially
	props, _ := queue.Properties(ctx, params)
	t.Logf("immediately after produce: scheduled=%d, pending=%d", props.ScheduledMessagesCount, props.PendingMessagesCount)

	// Start consumer
	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		t.Logf("consumed: %s", m.Body)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Wait for delay + scheduler (5s interval) + consumption
	time.Sleep(10 * time.Second)

	count := consumed.Load()
	t.Logf("consumed: %d messages", count)

	props, _ = queue.Properties(ctx, params)
	t.Logf("after wait: scheduled=%d, pending=%d", props.ScheduledMessagesCount, props.PendingMessagesCount)

	if count == 0 {
		t.Fatal("message was not consumed after delay")
	}
}

// Scenario: Zero delay — message is immediate
func TestDelay_ZeroDelay(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delay-zero")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("immediate").
		SetQueue(params).
		SetScheduledDelay(0)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Zero delay should be pending immediately
	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
	if props.ScheduledMessagesCount != 0 {
		t.Errorf("scheduled = %d, want 0", props.ScheduledMessagesCount)
	}
}

// Scenario: Multiple messages with different delays
func TestDelay_MultipleDelays(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delay-multi")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	prod.Produce(ctx, msg.New().SetBody("d1").SetQueue(params).SetScheduledDelay(1*time.Hour))
	prod.Produce(ctx, msg.New().SetBody("d2").SetQueue(params).SetScheduledDelay(2*time.Hour))
	prod.Produce(ctx, msg.New().SetBody("d3").SetQueue(params).SetScheduledDelay(30*time.Minute))

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 3 {
		t.Errorf("scheduled = %d, want 3", props.ScheduledMessagesCount)
	}
}

// Scenario: Browse scheduled delayed messages
func TestDelay_BrowseScheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delay-browse")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("delayed").
			SetQueue(params).
			SetScheduledDelay(time.Duration(i+1)*time.Hour),
		)
	}

	result, err := queue.BrowseMessages(ctx, params, &q.BrowseParams{
		Filter: q.BrowseScheduled,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 5 {
		t.Fatalf("scheduled = %d, want 5", result.Total)
	}
}

// Scenario: Delay + immediate messages in same queue
func TestDelay_MixedImmediateAndDelayed(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-delay-mixed")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Immediate
	prod.Produce(ctx, msg.New().SetBody("immediate").SetQueue(params))

	// Delayed
	prod.Produce(ctx, msg.New().SetBody("delayed").SetQueue(params).SetScheduledDelay(1*time.Hour))

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}
