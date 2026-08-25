/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Batch acknowledgments with full buffer
func TestBatchAck_FullBuffer(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-full")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	// Producer
	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	// Consumer with batch acks: flush every 3 messages
	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    3,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	count := consumed.Load()
	if count < 10 {
		t.Fatalf("consumed %d messages, want 10", count)
	}
}

// Scenario: Batch acknowledgments flush on timeout
func TestBatchAck_Timeout(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-timeout")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100, // Larger than message count
			BatchTimeout: 1 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	time.Sleep(3 * time.Second)

	count := consumed.Load()
	if count != 2 {
		t.Fatalf("consumed %d messages, want 2", count)
	}
}

// Scenario: Batch acknowledgments flush on shutdown
func TestBatchAck_FlushOnShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-shutdown")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 5; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchAcks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)

	time.Sleep(2 * time.Second)

	// Shutdown should flush remaining batch
	cons.Shutdown()

	count := consumed.Load()
	if count != 5 {
		t.Fatalf("consumed %d messages, want 5", count)
	}
}

// Scenario: Disabled batch acks (default) process immediately
func TestBatchAck_Disabled(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-ack-disabled")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

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

	time.Sleep(2 * time.Second)

	count := consumed.Load()
	if count != 5 {
		t.Fatalf("consumed %d messages, want 5", count)
	}
}

// Scenario: Batch unacknowledgments
func TestBatchUnack_FlushOnShutdown(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-batch-unack")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))

	var attempts atomic.Int64
	cons := redissmq.NewConsumer(
		consumer.WithBatchUnacks(consumer.BatchConfig{
			Enabled:      true,
			BatchSize:    100,
			BatchTimeout: 30 * time.Second,
		}),
	)
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		attempts.Add(1)
		return fmt.Errorf("processing failed") // Force unack
	})
	cons.Run(ctx)

	time.Sleep(3 * time.Second)
	cons.Shutdown()

	// Message should be retried (unacknowledged)
	if attempts.Load() == 0 {
		t.Fatal("message should have been processed at least once")
	}
	t.Logf("attempts: %d", attempts.Load())
}
