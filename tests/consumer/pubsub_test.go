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
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
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

// Scenario: PendingWithGroup uses group-specific pending list
func TestPubSub_PendingWithGroupUsesGroupKey(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-pending-with-group")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	groupID := "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	cgm.Save(ctx, params, groupID)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("hello").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
	messageID := ids[0]

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	pendingKey := qKey.PendingWithGroup(groupID)

	// Verify message is in group-specific pending list.
	length, err := redis.Client().LLen(ctx, pendingKey).Result()
	if err != nil {
		t.Fatalf("llen pending with group: %v", err)
	}
	if length != 1 {
		t.Fatalf("pending list length = %d, want 1", length)
	}
	idsInList, err := redis.Client().LRange(ctx, pendingKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("lrange pending with group: %v", err)
	}
	if len(idsInList) != 1 || idsInList[0] != messageID {
		t.Fatalf("pending list contents = %v, want [%s]", idsInList, messageID)
	}

	// Consume the message.
	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for consumption")
		default:
			if consumed.Load() == 1 {
				goto consumedDone
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
consumedDone:

	// Verify group pending list is now empty.
	length, err = redis.Client().LLen(ctx, pendingKey).Result()
	if err != nil {
		t.Fatalf("llen after consume: %v", err)
	}
	if length != 0 {
		t.Fatalf("pending list length after consume = %d, want 0", length)
	}
}

// Scenario: PriorityWithGroup uses group-specific priority sorted set
func TestPubSub_PriorityWithGroupUsesGroupKey(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-pubsub-priority-with-group")
	testutil.CreateQueue(t, ctx, params, queue.TypePriority, queue.DeliveryPubSub)

	groupID := "sms-service"
	cgm := redissmq.NewConsumerGroupManager()
	cgm.Save(ctx, params, groupID)

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("priority").SetQueue(params).SetPriority(msg.PriorityHigh)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}
	messageID := ids[0]

	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	priorityKey := qKey.PriorityWithGroup(groupID)

	// Verify message is in group-specific priority sorted set.
	count, err := redis.Client().ZCard(ctx, priorityKey).Result()
	if err != nil {
		t.Fatalf("zcard priority with group: %v", err)
	}
	if count != 1 {
		t.Fatalf("priority set size = %d, want 1", count)
	}
	idsInSet, err := redis.Client().ZRange(ctx, priorityKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange priority with group: %v", err)
	}
	if len(idsInSet) != 1 || idsInSet[0] != messageID {
		t.Fatalf("priority set contents = %v, want [%s]", idsInSet, messageID)
	}

	// Consume the message.
	var consumed atomic.Int64
	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error {
		consumed.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for consumption")
		default:
			if consumed.Load() == 1 {
				goto priorityConsumed
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
priorityConsumed:

	// Verify group priority set is now empty.
	count, err = redis.Client().ZCard(ctx, priorityKey).Result()
	if err != nil {
		t.Fatalf("zcard after consume: %v", err)
	}
	if count != 0 {
		t.Fatalf("priority set size after consume = %d, want 0", count)
	}
}
