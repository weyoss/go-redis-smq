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
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Schedule a repeating message
func TestRepeat_SingleRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-single")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("repeat").
		SetQueue(params).
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// First delivery should be immediate (no delay), subsequent are scheduled
	props, err := redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	// First message is pending (immediate), the rest will be scheduled
	if props.MessagesCount != 1 {
		t.Errorf("messages = %d, want 1", props.MessagesCount)
	}
}

// Scenario: Repeat with initial delay
func TestRepeat_WithDelay(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-delay")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("repeat-delay").
		SetQueue(params).
		SetScheduledDelay(10 * time.Second).
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be scheduled (has delay)
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: SetScheduledRepeat(0) delivers the message exactly once.
func TestRepeat_ZeroMeansSingleDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 15*time.Second)
	defer cancel()

	params := publicqueue.MustQueueParams("test-repeat-zero-single")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("single-delivery").
		SetQueue(params).
		SetScheduledRepeat(0).                     // zero = no repeats
		SetScheduledRepeatPeriod(60 * time.Second) // ignored: 0 repeat governs

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// ── Produce-time ──
	// Repeat count 0 makes the envelope non-schedulable, so
	// NextScheduledTimestamp returns 0 and the message is published as
	// Pending immediately — not deferred into the scheduled set.
	status, err := redissmq.NewMessageManager().Status(ctx, ids[0])
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status != msg.StatusPending {
		t.Errorf("status = %s, want PENDING "+
			"(0-repeat must deliver immediately, not enter the scheduled set)",
			status)
	}

	props, err := redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
	if props.ScheduledMessagesCount != 0 {
		t.Errorf("scheduled = %d, want 0 (no repeat cycle means no scheduled entry)",
			props.ScheduledMessagesCount)
	}

	// ── Consume-time ──
	var receivedMsg *msg.Transferable
	receivedCh := make(chan *msg.Transferable, 10)
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		receivedCh <- m
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	select {
	case receivedMsg = <-receivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: message was not delivered")
	}

	// ScheduledRepeatCount tracks how many repeat ticks have fired. If
	// the repeat path were engaged (even "indefinitely"), this would be
	// > 0 after the first delivery. It must stay 0.
	if receivedMsg.MessageState.ScheduledRepeatCount != 0 {
		t.Errorf("ScheduledRepeatCount = %d, want 0 "+
			"(message must not enter the repeat cycle)",
			receivedMsg.MessageState.ScheduledRepeatCount)
	}
	// Single delivery means exactly one checkout — attempts == 1.
	if receivedMsg.MessageState.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (single delivery, no retries)",
			receivedMsg.MessageState.Attempts)
	}

	// ── Post-consume ──
	// Any repeat tick would either push a clone into Scheduled (visible
	// via ScheduledMessagesCount) or into Pending (visible as a second
	// handler invocation). A short settle window catches anomalies that
	// fire immediately; the counter checks below catch anything that
	// lands after the window.
	select {
	case extra := <-receivedCh:
		t.Fatalf("unexpected second delivery (id=%s); "+
			"0-repeat must deliver exactly once", extra.ID)
	case <-time.After(2 * time.Second):
	}

	props, err = redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties (post-consume): %v", err)
	}
	if props.ScheduledMessagesCount != 0 {
		t.Errorf("scheduled after consume = %d, want 0", props.ScheduledMessagesCount)
	}
	if props.MessagesCount != 1 {
		t.Errorf("total messages = %d, want 1 (no repeat clone spawned)",
			props.MessagesCount)
	}
}

// Scenario: Repeat without period defaults to immediate repeats
func TestRepeat_NoPeriod(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-no-period")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("no-period").
		SetQueue(params).
		SetScheduledRepeat(3)
	// No period set

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Browse messages with repeat scheduling
func TestRepeat_Browse(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-browse")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	for i := 0; i < 3; i++ {
		prod.Produce(ctx, msg.New().
			SetBody("repeat").
			SetQueue(params).
			SetScheduledDelay(time.Duration(i+1)*time.Hour).
			SetScheduledRepeat(5).
			SetScheduledRepeatPeriod(60*time.Second),
		)
	}

	result, err := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseScheduled,
	})
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if result.Total != 3 {
		t.Fatalf("scheduled = %d, want 3", result.Total)
	}
}

// Scenario: Reset repeat params
func TestRepeat_ResetParams(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-reset")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("reset").
		SetQueue(params).
		SetScheduledRepeat(5).
		SetScheduledRepeatPeriod(60 * time.Second)

	m.ResetScheduledParams()

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be pending (immediate), not scheduled
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1", props.PendingMessagesCount)
	}
	if props.ScheduledMessagesCount != 0 {
		t.Errorf("scheduled = %d, want 0", props.ScheduledMessagesCount)
	}
}

// Scenario: Repeat count reached limit — schedule ends
func TestRepeat_CountExhausted(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-exhausted")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("exhausted").
		SetQueue(params).
		SetScheduledRepeat(1). // Only 1 repeat
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	// First delivery is pending, after repeat count exhausted, no more scheduled
	t.Logf("messages: %d, scheduled: %d, pending: %d",
		props.MessagesCount, props.ScheduledMessagesCount, props.PendingMessagesCount)
}
