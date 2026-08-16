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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Benchmark: Produce 10,000 messages
func BenchmarkProducer_10K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-producer-10k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(b, ctx)

	messageCount := 10_000

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < messageCount; i++ {
		m := msg.New().SetBody("benchmark-message").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			b.Fatalf("produce %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: Produce 100,000 messages
func BenchmarkProducer_100K(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-producer-100k-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(b, ctx)

	messageCount := 100_000

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < messageCount; i++ {
		m := msg.New().SetBody("benchmark-message").SetQueue(params)
		if _, err := prod.Produce(ctx, m); err != nil {
			b.Fatalf("produce %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: Produce with different body sizes
func BenchmarkProducer_BodySizes(b *testing.B) {
	sizes := []struct {
		name string
		body string
	}{
		{"100B", string(make([]byte, 100))},
		{"1KB", string(make([]byte, 1000))},
		{"10KB", string(make([]byte, 10000))},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			ctx := testutil.Setup(b) // Fresh Redis per sub-benchmark
			params := q.MustQueueParams(fmt.Sprintf("bench-body-%s-%d", size.name, time.Now().UnixNano()))
			testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)
			prod := testutil.StartProducer(b, ctx)

			messageCount := 5_000

			b.ResetTimer()
			start := time.Now()

			for i := 0; i < messageCount; i++ {
				m := msg.New().SetBody(size.body).SetQueue(params)
				if _, err := prod.Produce(ctx, m); err != nil {
					b.Fatalf("produce: %v", err)
				}
			}

			elapsed := time.Since(start)
			throughput := float64(messageCount) / elapsed.Seconds()
			b.ReportMetric(throughput, "msg/s")
		})
	}
}

// Benchmark: Produce via exchange
func BenchmarkProducer_ViaExchange(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-producer-ex-q-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams(fmt.Sprintf("bench-ex-%d", time.Now().UnixNano()), x.TypeDirect)
	dx := exchange.NewDirectExchange()
	if err := dx.BindQueue(ctx, params, exchangeParams, "bench.key"); err != nil {
		b.Fatalf("bind: %v", err)
	}

	prod := testutil.StartProducer(b, ctx)

	messageCount := 10_000

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < messageCount; i++ {
		m := msg.New().
			SetBody("benchmark").
			SetDirectExchange(exchangeParams).
			SetExchangeRoutingKey("bench.key")
		if _, err := prod.Produce(ctx, m); err != nil {
			b.Fatalf("produce %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(messageCount) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}

// Benchmark: Multi-producer throughput
func BenchmarkProducer_MultiProducer(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-multi-prod-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	producerCount := 4
	messagesPerProducer := 25_000
	totalMessages := producerCount * messagesPerProducer

	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	for p := 0; p < producerCount; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
	wg.Wait()

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(totalMessages) / elapsed.Seconds()
	b.ReportMetric(throughput, "msg/s")
}
