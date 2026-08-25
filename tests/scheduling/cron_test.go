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

// Scenario: Schedule a message with CRON expression
func TestCron_ScheduleWithCron(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-schedule")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("cron-msg").
		SetQueue(params).
		SetScheduledCron("0 0 10 * * *") // Daily at 10 AM

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Message should be in scheduled, not pending
	props, err := redissmq.NewQueueManager().Properties(ctx, params)
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

// Scenario: Invalid CRON expression is silently ignored
func TestCron_InvalidCronIgnored(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-invalid")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("invalid-cron").
		SetQueue(params).
		SetScheduledCron("invalid-cron-expression")

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Message should be in pending (cron was ignored, treated as immediate)
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	t.Logf("scheduled: %d, pending: %d", props.ScheduledMessagesCount, props.PendingMessagesCount)
}

// Scenario: Browse scheduled CRON messages
func TestCron_BrowseScheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-browse")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	for i := 0; i < 3; i++ {
		m := msg.New().
			SetBody("cron").
			SetQueue(params).
			SetScheduledCron("0 0 * * *") // Every hour
		prod.Produce(ctx, m)
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

// Scenario: Delete a scheduled message
func TestCron_DeleteScheduled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-delete")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	ids, _ := prod.Produce(ctx, msg.New().
		SetBody("delete-me").
		SetQueue(params).
		SetScheduledCron("0 0 * * *"),
	)

	// Delete the message
	result, err := redissmq.NewMessageManager().Delete(ctx, ids[0])
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if result.Status != msg.DeleteStatusOK {
		t.Fatalf("delete status: %s, want OK", result.Status)
	}

	// Should no longer be in scheduled
	scheduledResult, _ := redissmq.NewQueueManager().BrowseMessages(ctx, params, &publicqueue.BrowseParams{
		Filter: publicqueue.BrowseScheduled,
	})
	if scheduledResult.Total != 0 {
		t.Errorf("scheduled = %d, want 0 after delete", scheduledResult.Total)
	}
}

// Scenario: CRON with repeat
func TestCron_CronWithRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-repeat")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("cron-repeat").
		SetQueue(params).
		SetScheduledCron("0 0 * * *"). // Every hour
		SetScheduledRepeat(3).         // Repeat 3 times
		SetScheduledRepeatPeriod(60 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	// Message should be scheduled
	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.ScheduledMessagesCount != 1 {
		t.Errorf("scheduled = %d, want 1", props.ScheduledMessagesCount)
	}
}

// Scenario: Common CRON expressions
func TestCron_CommonExpressions(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-common")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	expressions := []struct {
		name string
		cron string
	}{
		{"every-minute", "* * * * *"},
		{"every-hour", "0 * * * *"},
		{"daily-midnight", "0 0 * * *"},
		{"daily-noon", "0 12 * * *"},
		{"weekly-monday", "0 0 * * 1"},
		{"monthly-first", "0 0 1 * *"},
		{"weekdays-9am", "0 9 * * 1-5"},
	}

	for _, expr := range expressions {
		m := msg.New().
			SetBody(expr.name).
			SetQueue(params).
			SetScheduledCron(expr.cron)

		_, err := prod.Produce(ctx, m)
		if err != nil {
			t.Errorf("%s: %v", expr.name, err)
		}
	}

	props, _ := redissmq.NewQueueManager().Properties(ctx, params)
	if props.ScheduledMessagesCount != int64(len(expressions)) {
		t.Errorf("scheduled = %d, want %d", props.ScheduledMessagesCount, len(expressions))
	}
}

// Scenario: Reset scheduled params on CRON message
func TestCron_ResetParams(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-cron-reset")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("reset-me").
		SetQueue(params).
		SetScheduledCron("0 0 * * *").
		SetScheduledRepeat(5)

	// Reset all scheduling
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
	if props.ScheduledMessagesCount != 0 {
		t.Errorf("scheduled = %d, want 0 (reset)", props.ScheduledMessagesCount)
	}
	if props.PendingMessagesCount != 1 {
		t.Errorf("pending = %d, want 1 (immediate after reset)", props.PendingMessagesCount)
	}
}
