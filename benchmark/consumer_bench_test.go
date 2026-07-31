/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package benchmark_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Benchmark: Consume 10,000 messages
func BenchmarkConsumer_10K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-consumer-10k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Pre-produce messages
	prod := testutil.StartProducer(b, ctx)
	messageCount := 10_000
	for i := 0; i < messageCount; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("benchmark").SetQueue(params)); err != nil {
			b.Fatalf("produce: %v", err)
		}
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		b.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	// Wait for consumer to start
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	// Wait for all messages to be consumed
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			b.Fatalf("timeout: consumed %d/%d", consumed.Load(), messageCount)
		default:
			if consumed.Load() >= int64(messageCount) {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
done:
	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: Consume 100,000 messages
func BenchmarkConsumer_100K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-consumer-100k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(b, ctx)
	messageCount := 100_000
	for i := 0; i < messageCount; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("benchmark").SetQueue(params)); err != nil {
			b.Fatalf("produce: %v", err)
		}
	}

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		b.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	deadline := time.After(60 * time.Second)
	for {
		select {
		case <-deadline:
			b.Fatalf("timeout: consumed %d/%d", consumed.Load(), messageCount)
		default:
			if consumed.Load() >= int64(messageCount) {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
done:
	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: Multi-consumer throughput
func BenchmarkConsumer_MultiConsumer(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-multi-cons-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(b, ctx)
	messageCount := 50_000
	for i := 0; i < messageCount; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("benchmark").SetQueue(params)); err != nil {
			b.Fatalf("produce: %v", err)
		}
	}

	var consumed atomic.Int64
	consumerCount := 4

	for i := 0; i < consumerCount; i++ {
		cons := redissmq.NewConsumer()
		cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
			consumed.Add(1)
			return nil
		})
		if err := cons.Run(ctx); err != nil {
			b.Fatalf("run consumer %d: %v", i, err)
		}
		defer cons.Shutdown()
	}

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			b.Fatalf("timeout: consumed %d/%d", consumed.Load(), messageCount)
		default:
			if consumed.Load() >= int64(messageCount) {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
done:
	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}
