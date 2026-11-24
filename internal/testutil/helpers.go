/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package testutil

import (
	"context"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	internalworker "github.com/weyoss/go-redis-smq/internal/worker"
	"github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// CreateQueue is a test helper that creates a queue using the internal
// queue manager implementation. This is appropriate for internal test
// utilities and avoids dependency on public convenience functions.
func CreateQueue(tb testing.TB, ctx context.Context, params *publicqueue.Params, queueType publicqueue.Type, deliveryModel publicqueue.DeliveryModel) {
	tb.Helper()
	qm := internalqueue.NewManager()
	if err := qm.Create(ctx, params, queueType, deliveryModel); err != nil {
		tb.Fatalf("create queue %s: %v", params.Name(), err)
	}
}

// StartProducer is a test helper that starts a producer and registers cleanup.
// It returns the public producer interface.
func StartProducer(tb testing.TB, ctx context.Context) producer.Producer {
	tb.Helper()
	prod := redissmq.NewProducer()
	if err := prod.Run(ctx); err != nil {
		tb.Fatalf("run producer: %v", err)
	}
	tb.Cleanup(func() { prod.Shutdown(ctx) })
	return prod
}

// WorkerIntervals returns the intervals configured for the current test
// run. Tests that poll for interval-driven events use this to scale their
// deadlines instead of hardcoding durations. A change to TestIntervals
// then automatically updates every deadline.
func WorkerIntervals() internalworker.Intervals {
	return internalworker.TestIntervals()
}

// IntervalDeadline returns a generous deadline for a condition gated on
// the given interval: 20 ticks, capped at 2 minutes. Twenty ticks is
// enough to catch a bug that fires on the first few executions without
// making flaky CI wait indefinitely.
func IntervalDeadline(interval time.Duration) time.Duration {
	d := min(20*interval, 2*time.Minute)
	return d
}
