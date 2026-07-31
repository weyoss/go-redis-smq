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
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Benchmark: Produce and consume 10,000 messages end-to-end
func BenchmarkCombined_10K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-combined-10k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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

	prod := testutil.StartProducer(b, ctx)
	messageCount := 10_000

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	go func() {
		for i := 0; i < messageCount; i++ {
			m := msg.New().SetBody("benchmark").SetQueue(params)
			if _, err := prod.Produce(ctx, m); err != nil {
				b.Errorf("produce: %v", err)
				return
			}
		}
	}()

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

// Benchmark: Produce and consume 100,000 messages end-to-end
func BenchmarkCombined_100K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-combined-100k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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

	prod := testutil.StartProducer(b, ctx)
	messageCount := 100_000

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	go func() {
		for i := 0; i < messageCount; i++ {
			m := msg.New().SetBody("benchmark").SetQueue(params)
			if _, err := prod.Produce(ctx, m); err != nil {
				b.Errorf("produce: %v", err)
				return
			}
		}
	}()

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

// Benchmark: Concurrent producers and consumers
func BenchmarkCombined_Concurrent(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-concurrent-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

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

	producerCount := 4
	messagesPerProducer := 25_000
	totalMessages := producerCount * messagesPerProducer

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	start := time.Now()

	for p := 0; p < producerCount; p++ {
		go func() {
			prod := testutil.StartProducer(b, ctx)
			for i := 0; i < messagesPerProducer; i++ {
				m := msg.New().SetBody("benchmark").SetQueue(params)
				if _, err := prod.Produce(ctx, m); err != nil {
					b.Errorf("produce: %v", err)
					return
				}
			}
		}()
	}

	deadline := time.After(60 * time.Second)
	for {
		select {
		case <-deadline:
			b.Fatalf("timeout: consumed %d/%d", consumed.Load(), totalMessages)
		default:
			if consumed.Load() >= int64(totalMessages) {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
done:
	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(totalMessages) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: PubSub throughput
func BenchmarkCombined_PubSub(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-pubsub-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPubSub)

	queue.SaveConsumerGroup(ctx, params, "group-a")
	queue.SaveConsumerGroup(ctx, params, "group-b")

	var consumedA, consumedB atomic.Int64

	consA := redissmq.NewConsumer()
	consA.ConsumeWithGroup(params, "group-a", func(ctx context.Context, m *msg.Transferable) error {
		consumedA.Add(1)
		return nil
	})
	consA.Run(ctx)
	defer consA.Shutdown()

	consB := redissmq.NewConsumer()
	consB.ConsumeWithGroup(params, "group-b", func(ctx context.Context, m *msg.Transferable) error {
		consumedB.Add(1)
		return nil
	})
	consB.Run(ctx)
	defer consB.Shutdown()

	prod := testutil.StartProducer(b, ctx)
	messageCount := 10_000

	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		consumedA.Store(0)
		consumedB.Store(0)

		go func() {
			for j := 0; j < messageCount; j++ {
				m := msg.New().SetBody("benchmark").SetQueue(params)
				if _, err := prod.Produce(ctx, m); err != nil {
					b.Errorf("produce: %v", err)
					return
				}
			}
		}()

		expectedTotal := int64(messageCount * 2) // 2 groups
		deadline := time.After(30 * time.Second)
		for {
			select {
			case <-deadline:
				b.Fatalf("timeout: A=%d, B=%d, total=%d, want=%d",
					consumedA.Load(), consumedB.Load(),
					consumedA.Load()+consumedB.Load(), expectedTotal)
			default:
				if consumedA.Load()+consumedB.Load() >= expectedTotal {
					goto next
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	next:
	}

	b.StopTimer()
	elapsed := b.Elapsed()

	totalMessages := int64(messageCount * 2 * b.N)
	throughput := float64(totalMessages) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}
