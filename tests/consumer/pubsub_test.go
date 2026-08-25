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
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: PubSub delivers to all consumer groups
func TestPubSub_DeliveryToAllGroups(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-delivery")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	cgm.Save(ctx, params, "email-service")
	cgm.Save(ctx, params, "sms-service")

	prod := testutil.StartProducer(t, ctx)

	var emailCount, smsCount atomic.Int64

	emailCons := redissmq.NewConsumer()
	emailCons.ConsumeWithGroup(params, "email-service", func(ctx context.Context, m *msg.Transferable) error {
		emailCount.Add(1)
		return nil
	})
	emailCons.Run(ctx)
	defer emailCons.Shutdown()

	smsCons := redissmq.NewConsumer()
	smsCons.ConsumeWithGroup(params, "sms-service", func(ctx context.Context, m *msg.Transferable) error {
		smsCount.Add(1)
		return nil
	})
	smsCons.Run(ctx)
	defer smsCons.Shutdown()

	// Produce one message — both groups receive it
	ids, err := prod.Produce(ctx, msg.New().SetBody("alert").SetQueue(params))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 message IDs (one per group), got %d: %v", len(ids), ids)
	}

	time.Sleep(3 * time.Second)

	if emailCount.Load() != 1 {
		t.Errorf("email group: %d messages, want 1", emailCount.Load())
	}
	if smsCount.Load() != 1 {
		t.Errorf("sms group: %d messages, want 1", smsCount.Load())
	}
}

// Scenario: PubSub load balances within a group
func TestPubSub_LoadBalanceWithinGroup(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-balance")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	redissmq.NewConsumerGroupManager().Save(ctx, params, "workers")

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("task").SetQueue(params))
	}

	var worker1Count, worker2Count atomic.Int64

	w1 := redissmq.NewConsumer()
	w1.ConsumeWithGroup(params, "workers", func(ctx context.Context, m *msg.Transferable) error {
		worker1Count.Add(1)
		return nil
	})
	w1.Run(ctx)
	defer w1.Shutdown()

	w2 := redissmq.NewConsumer()
	w2.ConsumeWithGroup(params, "workers", func(ctx context.Context, m *msg.Transferable) error {
		worker2Count.Add(1)
		return nil
	})
	w2.Run(ctx)
	defer w2.Shutdown()

	time.Sleep(5 * time.Second)

	total := worker1Count.Load() + worker2Count.Load()
	if total != 10 {
		t.Errorf("total consumed: %d, want 10", total)
	}
	if worker1Count.Load() == 0 || worker2Count.Load() == 0 {
		t.Errorf("load balancing not working: w1=%d, w2=%d", worker1Count.Load(), worker2Count.Load())
	}
	t.Logf("worker1: %d, worker2: %d", worker1Count.Load(), worker2Count.Load())
}

// Scenario: Consuming without group on PubSub creates ephemeral group
func TestPubSub_EphemeralGroup(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-ephemeral")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)

	prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	time.Sleep(3 * time.Second)

	if consumed.Load() != 1 {
		t.Errorf("consumed: %d, want 1", consumed.Load())
	}

	cgm := redissmq.NewConsumerGroupManager()

	// Ephemeral group should be auto-created
	groups, _ := cgm.List(ctx, params)
	t.Logf("groups after ephemeral: %v", groups)
	if len(groups) != 1 {
		t.Errorf("expected 1 ephemeral group, got %d: %v", len(groups), groups)
	}

	// Shutdown consumer — ephemeral group should be deleted
	cons.Shutdown()
	time.Sleep(500 * time.Millisecond)

	groups, _ = cgm.List(ctx, params)
	t.Logf("groups after shutdown: %v", groups)
	if len(groups) != 0 {
		t.Errorf("ephemeral group should be deleted on shutdown, got %v", groups)
	}
}

// Scenario: Cancel group on a running consumer
func TestPubSub_CancelGroup(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-cancel")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	redissmq.NewConsumerGroupManager().Save(ctx, params, "email-service")

	prod := testutil.StartProducer(t, ctx)

	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, "email-service", func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	cons.Run(ctx)
	defer cons.Shutdown()

	prod.Produce(ctx, msg.New().SetBody("before-cancel").SetQueue(params))
	time.Sleep(2 * time.Second)
	beforeCancel := consumed.Load()
	t.Logf("consumed before cancel: %d", beforeCancel)

	// Cancel the group
	cons.CancelWithGroup(params, "email-service")
	time.Sleep(1 * time.Second)

	prod.Produce(ctx, msg.New().SetBody("after-cancel").SetQueue(params))
	time.Sleep(3 * time.Second)

	afterCancel := consumed.Load()
	t.Logf("consumed after cancel: %d", afterCancel)

	if afterCancel > beforeCancel {
		t.Fatal("messages consumed after group cancelled")
	}
}
