/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Cancel(queue) on an ephemeral Pub/Sub handler must remove
// BOTH the config and the running instance. Pre-fix, exact matching on
// the configured (empty) group found the config but not the instance —
// the config was removed and the instance orphaned, so the handler kept
// consuming with no way for Queues() or Cancel to ever see it again.
func TestCancel_EphemeralPubSubHandler(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-g-cancel-ephemeral")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	// Sanity: handler is registered.
	if len(cons.Queues()) != 1 {
		t.Fatalf("Queues() = %d entries, want 1", len(cons.Queues()))
	}

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("before").SetQueue(params))
	time.Sleep(2 * time.Second)
	if consumed.Load() == 0 {
		t.Fatal("handler not consuming before cancel")
	}

	// Cancel by queue name (no group — matches how the public API is
	// documented to work on a Pub/Sub consumer that was subscribed via
	// Consume(queue) rather than ConsumeWithGroup).
	cons.Cancel(params)
	time.Sleep(200 * time.Millisecond)

	// Config gone.
	if got := len(cons.Queues()); got != 0 {
		t.Errorf("Queues() = %d entries, want 0 after cancel", got)
	}

	// Instance gone — no further consumption. This is the part the
	// pre-fix behavior failed: the instance kept running.
	beforeCount := consumed.Load()
	prod.Produce(ctx, msg.New().SetBody("after").SetQueue(params))
	time.Sleep(2 * time.Second)

	if delta := consumed.Load() - beforeCount; delta > 0 {
		t.Errorf("consumed %d additional messages after Cancel(queue); "+
			"handler instance was orphaned (G)", delta)
	}
}

// Scenario: Pause on an ephemeral Pub/Sub handler must actually stop
// consumption, and resume must restart it. Pre-fix, onQueuePaused and
// onQueueActive both passed cfg.groupID (empty) to StopHandler /
// StartHandler, so the ephemeral instance was never stopped and
// resume created a duplicate — either way, the queue's pause had no
// effect on the ephemeral handler.
func TestPause_EphemeralPubSubHandler(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-g-pause-ephemeral")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	prod := testutil.StartProducer(t, ctx)
	prod.Produce(ctx, msg.New().SetBody("before").SetQueue(params))
	time.Sleep(2 * time.Second)
	beforePause := consumed.Load()
	if beforePause == 0 {
		t.Fatal("handler not consuming before pause")
	}

	// Pause the queue. Pre-fix: StopHandler couldn't find the instance
	// and returned false — the handler kept consuming.
	sm := redissmq.NewStateManager()
	if _, err := sm.Pause(ctx, params, nil); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Produce while paused. Should NOT be consumed.
	_, err := prod.Produce(ctx, msg.New().SetBody("during-pause").SetQueue(params))
	if err != nil {
		t.Fatalf("produce during pause: %v (ephemeral group was deleted at "+
			"pause time; StopHandler must preserve the group)", err)
	}
	time.Sleep(3 * time.Second)

	if delta := consumed.Load() - beforePause; delta > 0 {
		t.Errorf("consumed %d messages while paused; "+
			"onQueuePaused did not stop the ephemeral handler (G)", delta)
	}

	// Resume. Handler should restart and drain the buffered message.
	if _, err := sm.Resume(ctx, params, nil); err != nil {
		t.Fatalf("resume: %v", err)
	}
	time.Sleep(5 * time.Second)

	if consumed.Load() <= beforePause {
		t.Errorf("consumed %d messages after resume (want > %d); "+
			"onQueueActive did not restart the ephemeral handler (G)",
			consumed.Load(), beforePause)
	}
}
