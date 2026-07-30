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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Invalid CRON expression is silently ignored
func TestError_InvalidCronIgnored(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-invalid-cron")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	invalidCrons := []string{
		"invalid",
		"* * * * * * *", // Too many fields
		"* * * *",       // Too few fields
		"60 * * * *",    // Invalid minute
		"* 24 * * *",    // Invalid hour
	}

	for _, cron := range invalidCrons {
		m := msg.New().
			SetBody("invalid").
			SetQueue(params).
			SetScheduledCron(cron)

		ids, err := prod.Produce(ctx, m)
		if err != nil {
			t.Fatalf("produce with cron %q: %v", cron, err)
		}
		if len(ids) != 1 {
			t.Fatalf("expected 1 message ID, got %d", len(ids))
		}
	}
}

// Scenario: Invalid CRON with repeat still delivers immediately
func TestError_InvalidCronWithRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-invalid-cron-repeat")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("invalid-cron-repeat").
		SetQueue(params).
		SetScheduledCron("invalid").
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

// Scenario: Negative delay is treated as zero
func TestError_NegativeDelay(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-neg-delay")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("neg-delay").
		SetQueue(params).
		SetScheduledDelay(-1 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Negative repeat is treated as zero
func TestError_NegativeRepeat(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-neg-repeat")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("neg-repeat").
		SetQueue(params).
		SetScheduledRepeat(-1)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Negative repeat period is treated as zero
func TestError_NegativeRepeatPeriod(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-neg-period")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("neg-period").
		SetQueue(params).
		SetScheduledRepeat(3).
		SetScheduledRepeatPeriod(-1 * time.Second)

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Empty CRON string
func TestError_EmptyCron(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-empty-cron")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("empty-cron").
		SetQueue(params).
		SetScheduledCron("")

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}

// Scenario: Multiple resets
func TestError_MultipleResets(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-multi-reset")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().
		SetBody("multi-reset").
		SetQueue(params).
		SetScheduledCron("0 0 * * *")

	m.ResetScheduledParams()
	m.ResetScheduledParams()
	m.ResetScheduledParams()

	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
}
