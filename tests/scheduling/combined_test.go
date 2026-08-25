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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Delay + Repeat — first delivery delayed, then repeats
func TestCombined_DelayAndRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-delay-repeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("delay-repeat").
		SetQueue(params).
		SetScheduledDelay(30 * time.Second).
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be scheduled (delay takes precedence)
	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: CRON + Repeat — CRON triggers, repeat between ticks
func TestCombined_CronAndRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-cron-repeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("cron-repeat").
		SetQueue(params).
		SetScheduledCron("0 0 * * *"). // Every hour
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be scheduled (CRON)
	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: Delay + CRON — delay first, then CRON schedule
func TestCombined_DelayAndCron(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-delay-cron")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("delay-cron").
		SetQueue(params).
		SetScheduledDelay(10 * time.Second).
		SetScheduledCron("0 0 * * *")

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be scheduled (delay takes precedence)
	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: All scheduling options combined
func TestCombined_AllOptions(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-all")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("all-options").
		SetQueue(params).
		SetScheduledDelay(5 * time.Second).
		SetScheduledCron("0 0 * * *").
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(30 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: Reset all scheduling params
func TestCombined_ResetAllParams(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-reset-all")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("reset-all").
		SetQueue(params).
		SetScheduledDelay(10 * time.Second).
		SetScheduledCron("0 0 * * *").
		SetScheduledRepeat(5).
		SetScheduledRepeatPeriod(60 * time.Second)

	// Reset everything
	m.ResetScheduledParams()

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Should be pending (immediate), not scheduled
	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
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

// Scenario: Mixed scheduling — some immediate, some delayed, some CRON
func TestCombined_MixedScheduling(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-combined-mixed")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	// Immediate
	prod.Produce(ctx, msg.New().SetBody("immediate").SetQueue(params))

	// Delayed
	prod.Produce(ctx, msg.New().SetBody("delayed").SetQueue(params).SetScheduledDelay(1*time.Hour))

	// CRON
	prod.Produce(ctx, msg.New().SetBody("cron").SetQueue(params).SetScheduledCron("0 0 * * *"))

	// Repeat
	prod.Produce(ctx, msg.New().
		SetBody("repeat").
		SetQueue(params).
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(60*time.Second),
	)

	// Delay + Repeat
	prod.Produce(ctx, msg.New().
		SetBody("delay-repeat").
		SetQueue(params).
		SetScheduledDelay(30*time.Second).
		SetScheduledRepeat(2).
		SetScheduledRepeatPeriod(30*time.Second),
	)

	qm := redissmq.NewQueueManager()
	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}

	t.Logf("messages: %d, pending: %d, scheduled: %d",
		props.MessagesCount, props.PendingMessagesCount, props.ScheduledMessagesCount)

	if props.MessagesCount != 5 {
		t.Errorf("messages = %d, want 5", props.MessagesCount)
	}
	// Immediate + Repeat-first-delivery are pending
	if props.PendingMessagesCount < 1 {
		t.Errorf("pending = %d, want >= 1", props.PendingMessagesCount)
	}
	// Delayed + CRON + Delay-Repeat are scheduled
	if props.ScheduledMessagesCount < 3 {
		t.Errorf("scheduled = %d, want >= 3", props.ScheduledMessagesCount)
	}
}
