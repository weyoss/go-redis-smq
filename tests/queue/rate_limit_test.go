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

// Scenario: Rate limit throttles message consumption
func TestQueueRateLimit_ThrottlesConsumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-rate-throttle")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Set rate limit: 3 messages per 2 seconds
	rl := q.MustRateLimitParams(3, 2*time.Second)
	if err := queue.SetRateLimit(ctx, params, rl); err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Produce 20 messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 20; i++ {
		m := msg.New().SetBody("msg").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	// Consume with rate limiting
	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	time.Sleep(5 * time.Second)

	count := consumed.Load()
	if count == 0 {
		t.Fatal("no messages consumed")
	}
	// With rate limit of 3 per 2s, over 5s expect 6-9 messages
	if count > 9 {
		t.Fatalf("consumed %d messages, rate limit may not be working", count)
	}
}

// Scenario: Per-queue rate limit isolation
func TestQueueRateLimit_PerQueueIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fastQueue := q.MustQueueParams("test-rate-fast")
	slowQueue := q.MustQueueParams("test-rate-slow")
	testutil.CreateQueue(t, ctx, fastQueue, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, slowQueue, q.TypeFIFO, q.DeliveryPointToPoint)

	// Slow queue: 2 messages per 5 seconds
	rl := q.MustRateLimitParams(2, 5*time.Second)
	if err := queue.SetRateLimit(ctx, slowQueue, rl); err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Produce 5 messages to each queue
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("fast").SetQueue(fastQueue))
		prod.Produce(ctx, msg.New().SetBody("slow").SetQueue(slowQueue))
	}

	var fastCount, slowCount atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(fastQueue, func(ctx context.Context, m *msg.Transferable) error {
		fastCount.Add(1)
		return nil
	})
	cons.Consume(slowQueue, func(ctx context.Context, m *msg.Transferable) error {
		slowCount.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	time.Sleep(5 * time.Second)

	fast := fastCount.Load()
	slow := slowCount.Load()

	if fast == 0 {
		t.Fatal("fast queue should have consumed messages")
	}
	if fast != 5 {
		t.Errorf("fast queue consumed %d, expected 5", fast)
	}
	if slow > 2 {
		t.Errorf("slow queue consumed %d, expected <= 2 due to rate limit", slow)
	}
}

// Scenario: Clearing rate limit resumes normal consumption
func TestQueueRateLimit_ClearResumesConsumption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-rate-clear-resume")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Very restrictive rate limit
	rl := q.MustRateLimitParams(1, 30*time.Second)
	if err := queue.SetRateLimit(ctx, params, rl); err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Produce 5 messages
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	// Let one message through the rate limit
	time.Sleep(2 * time.Second)

	// Clear the rate limit
	if err := queue.ClearRateLimit(ctx, params); err != nil {
		t.Fatalf("clear rate limit: %v", err)
	}

	// Wait for remaining messages
	time.Sleep(2 * time.Second)

	count := consumed.Load()
	if count < 1 {
		t.Fatal("expected at least 1 message after clearing rate limit")
	}
}

// Scenario: CRUD operations for rate limits
func TestQueueRateLimit_CRUD(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-rate-crud")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// No rate limit initially
	got, err := queue.RateLimit(ctx, params)
	if err != nil {
		t.Fatalf("get rate limit: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil initially, got %+v", got)
	}

	// Set rate limit
	rl := q.MustRateLimitParams(100, time.Minute)
	err = queue.SetRateLimit(ctx, params, rl)
	if err != nil {
		t.Fatalf("set rate limit: %v", err)
	}

	// Verify
	got, err = queue.RateLimit(ctx, params)
	if err != nil {
		t.Fatalf("get rate limit after set: %v", err)
	}
	if got == nil {
		t.Fatal("rate limit was nil after set")
	}
	if got.Limit() != 100 || got.Interval() != time.Minute {
		t.Fatalf("got %d/%v, want 100/1m", got.Limit(), got.Interval())
	}

	// Clear
	err = queue.ClearRateLimit(ctx, params)
	if err != nil {
		t.Fatalf("clear rate limit: %v", err)
	}

	// Verify cleared
	got, err = queue.RateLimit(ctx, params)
	if err != nil {
		t.Fatalf("get rate limit after clear: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after clear, got %+v", got)
	}
}

// Scenario: Invalid rate limit parameters
func TestQueueRateLimit_InvalidParams(t *testing.T) {
	_, err := q.NewRateLimitParams(0, time.Minute)
	if err == nil {
		t.Fatal("expected error for limit <= 0")
	}

	_, err = q.NewRateLimitParams(100, 500*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for interval < 1s")
	}
}
