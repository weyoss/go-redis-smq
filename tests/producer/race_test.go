/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer_test

import (
	"sync"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Concurrent Produce + Shutdown on a Pub/Sub queue must never
// orphan a message on the queue-level Pending list. Pre-fix, cleanup
// cleared pubSubResolver while a Produce was mid-Redis, Resolve() returned
// (nil, false), and dispatch fell back to queue-level Pending — which no
// Pub/Sub consumer ever reads.
func TestProduce_ConcurrentShutdown_NoOrphanedMessages(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-c1-produce-shutdown-race")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, "workers"); err != nil {
		t.Fatalf("create group: %v", err)
	}

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	groupPendingKey := qKey.PendingWithGroup("workers")

	const iterations = 30
	for i := 0; i < iterations; i++ {
		prod := redissmq.NewProducer()
		if err := prod.Run(ctx); err != nil {
			t.Fatalf("iter %d: run: %v", i, err)
		}

		const produces = 8
		var wg sync.WaitGroup
		start := make(chan struct{})

		for j := 0; j < produces; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				// Outcome is intentionally ignored: a produce that loses
				// the race must fail cleanly with ErrNotRunning; a
				// produce that wins must dispatch to the group-level
				// Pending list. What must NOT happen is a silent orphan
				// on queue-level Pending.
				_, _ = prod.Produce(ctx, msg.New().SetBody("race").SetQueue(params))
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			prod.Shutdown(ctx)
		}()

		close(start)
		wg.Wait()

		// The invariant: nothing ever landed on queue-level Pending.
		pendingLen, err := redis.Client().LLen(ctx, qKey.Pending()).Result()
		if err != nil {
			t.Fatalf("iter %d: llen pending: %v", i, err)
		}
		if pendingLen != 0 {
			t.Fatalf("iter %d: queue-level Pending has %d entries — "+
				"a Produce/Shutdown race orphaned a message (C1). "+
				"This is the failure mode ErrNoConsumerGroups exists to "+
				"prevent; it became reachable again once cleanup cleared "+
				"pubSubResolver without waiting for in-flight Produce.",
				i, pendingLen)
		}

		// Reset group Pending between iterations so the assertion is
		// per-iteration, not cumulative.
		if err := redis.Client().Del(ctx, groupPendingKey).Err(); err != nil {
			t.Fatalf("iter %d: del group pending: %v", i, err)
		}

		// Small pause so goroutines fully drain before the next iteration
		// creates a fresh producer. Not a correctness requirement; just
		// keeps the -race detector output clean.
		time.Sleep(time.Millisecond)
	}
}
