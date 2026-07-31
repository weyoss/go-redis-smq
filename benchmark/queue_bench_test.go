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
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Benchmark: Create 1,000 queues
func BenchmarkQueue_Create(b *testing.B) {
	ctx := testutil.Setup(b)

	b.ResetTimer()
	start := time.Now()

	count := 1000
	for i := 0; i < count; i++ {
		params := q.MustQueueParams(fmt.Sprintf("bench-queue-create-%d-%d", time.Now().UnixNano(), i))
		if err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
			b.Fatalf("create queue %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(count) / elapsed.Seconds()
	b.ReportMetric(throughput, "queues/s")
}

// Benchmark: Browse 10,000 messages
func BenchmarkQueue_Browse(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-browse-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(b, ctx)
	messageCount := 10_000
	for i := 0; i < messageCount; i++ {
		if _, err := prod.Produce(ctx, msg.New().SetBody("benchmark").SetQueue(params)); err != nil {
			b.Fatalf("produce: %v", err)
		}
	}

	b.ResetTimer()
	start := time.Now()

	iterations := 100
	for i := 0; i < iterations; i++ {
		_, err := queue.BrowseMessages(ctx, params, &q.BrowseParams{
			Filter: q.BrowsePublished,
			Offset: 0,
			Count:  100,
		})
		if err != nil {
			b.Fatalf("browse %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(iterations) / elapsed.Seconds()
	b.ReportMetric(throughput, "browses/s")
}

// Benchmark: Queue state transitions
func BenchmarkQueue_StateTransitions(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-state-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	b.ResetTimer()
	start := time.Now()

	iterations := 100
	for i := 0; i < iterations; i++ {
		if _, err := queue.Pause(ctx, params, nil); err != nil {
			b.Fatalf("pause %d: %v", i, err)
		}
		if _, err := queue.Resume(ctx, params, nil); err != nil {
			b.Fatalf("resume %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	transitions := iterations * 2
	throughput := float64(transitions) / elapsed.Seconds()
	b.ReportMetric(throughput, "transitions/s")
}

// Benchmark: Queue properties read
func BenchmarkQueue_Properties(b *testing.B) {
	ctx := testutil.Setup(b)
	params := q.MustQueueParams(fmt.Sprintf("bench-props-%d", time.Now().UnixNano()))
	testutil.CreateQueue(b, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	b.ResetTimer()
	start := time.Now()

	iterations := 1000
	for i := 0; i < iterations; i++ {
		if _, err := queue.Properties(ctx, params); err != nil {
			b.Fatalf("properties %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	throughput := float64(iterations) / elapsed.Seconds()
	b.ReportMetric(throughput, "reads/s")
}
