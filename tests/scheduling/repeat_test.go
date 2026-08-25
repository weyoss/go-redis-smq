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
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
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

// Scenario: Repeat count of 0 means indefinite
func TestRepeat_Indefinite(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-repeat-indefinite")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("indefinite").
		SetQueue(params).
		SetScheduledRepeat(0). // Indefinite
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.MessagesCount != 1 {
		t.Errorf("messages = %d, want 1", props.MessagesCount)
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
