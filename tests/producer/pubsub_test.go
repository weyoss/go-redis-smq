/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package producer_test

import (
	"errors"
	"testing"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Producing directly to a Pub/Sub queue with zero consumer groups
// must fail with ErrNoConsumerGroups instead of orphaning the message on the
// queue-level Pending list.
//
// Regression test for H2. Pre-fix, the produce succeeded but placed the
// message on the queue-level Pending list, which no consumer reads from
// (Pub/Sub consumers read PendingWithGroup(groupID)). The message was
// permanently orphaned.
func TestProduce_PubSubNoGroupsRejected(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-h2-pubsub-no-groups")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	// No consumer groups created.

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("orphan").SetQueue(params)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected ErrNoConsumerGroups, got nil")
	}
	if !errors.Is(err, publicproducer.ErrNoConsumerGroups) {
		t.Fatalf("err = %v, want ErrNoConsumerGroups", err)
	}

	// Verify nothing was published (no orphaned message on the queue).
	props, err := redissmq.NewQueueManager().Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.MessagesCount != 0 {
		t.Errorf("MessagesCount = %d, want 0 (orphan not published)", props.MessagesCount)
	}
	if props.PendingMessagesCount != 0 {
		t.Errorf("PendingMessagesCount = %d, want 0 (orphan not published)", props.PendingMessagesCount)
	}

	// Direct assertion on the queue-level Pending list — catches the case
	// where the counters are correct but a rogue write still landed.
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	pendingLen, err := redis.Client().LLen(ctx, qKey.Pending()).Result()
	if err != nil {
		t.Fatalf("llen pending: %v", err)
	}
	if pendingLen != 0 {
		t.Errorf("queue-level Pending has %d entries, want 0", pendingLen)
	}
}

// Scenario: Producing to a Pub/Sub queue with one or more consumer groups
// succeeds and produces one message per group.
//
// Confirms the H2 fix doesn't over-reject: valid Pub/Sub produces still work.
func TestProduce_PubSubWithGroupsSucceeds(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-h2-pubsub-with-groups")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, "group-a"); err != nil {
		t.Fatalf("save group-a: %v", err)
	}
	if _, err := cgm.Save(ctx, params, "group-b"); err != nil {
		t.Fatalf("save group-b: %v", err)
	}

	prod := testutil.StartProducer(t, ctx)

	m := msg.New().SetBody("fanout").SetQueue(params)
	ids, err := prod.Produce(ctx, m)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 message IDs (one per group), got %d: %v", len(ids), ids)
	}
}

// Scenario: Producing via an exchange where one bound queue is Pub/Sub with
// zero consumer groups rejects the whole produce, without partial success.
//
// The pre-flight check in produceToExchange prevents the scenario where some
// queues receive the message and others error mid-loop (which would produce
// duplicates on retry).
func TestProduce_ViaExchange_PubSubNoGroupsRejected(t *testing.T) {
	ctx := testutil.Setup(t)

	p2pQueue := publicqueue.MustQueueParams("test-h2-ex-p2p")
	testutil.CreateQueue(t, ctx, p2pQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	pubsubQueue := publicqueue.MustQueueParams("test-h2-ex-pubsub")
	testutil.CreateQueue(t, ctx, pubsubQueue, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)
	// No groups on pubsubQueue.

	exParams := exchange.MustExchangeParams("test-h2-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	if err := fx.BindQueue(ctx, p2pQueue, exParams); err != nil {
		t.Fatalf("bind p2p: %v", err)
	}
	if err := fx.BindQueue(ctx, pubsubQueue, exParams); err != nil {
		t.Fatalf("bind pubsub: %v", err)
	}

	prod := testutil.StartProducer(t, ctx)
	m := msg.New().SetBody("should-not-arrive").SetFanoutExchange(exParams)
	_, err := prod.Produce(ctx, m)
	if err == nil {
		t.Fatal("expected ErrNoConsumerGroups, got nil")
	}
	if !errors.Is(err, publicproducer.ErrNoConsumerGroups) {
		t.Fatalf("err = %v, want ErrNoConsumerGroups", err)
	}

	// Pre-flight must abort before any queue receives the message — verify
	// the P2P queue also has zero messages (no partial success).
	p2pProps, err := redissmq.NewQueueManager().Properties(ctx, p2pQueue)
	if err != nil {
		t.Fatalf("p2p properties: %v", err)
	}
	if p2pProps.MessagesCount != 0 {
		t.Errorf("P2P queue has %d messages, want 0 (partial success must not happen)",
			p2pProps.MessagesCount)
	}

	pubsubProps, err := redissmq.NewQueueManager().Properties(ctx, pubsubQueue)
	if err != nil {
		t.Fatalf("pubsub properties: %v", err)
	}
	if pubsubProps.MessagesCount != 0 {
		t.Errorf("PubSub queue has %d messages, want 0 (orphan not published)",
			pubsubProps.MessagesCount)
	}
}

// Scenario: The producer's resolver cache may be briefly stale after a group
// is created via SAdd but before the consumerGroupCreated event reaches the
// resolver. The fresh-Redis-check fallback in resolvePubSubTargets must
// consult Redis and produce successfully in that window.
//
// This is what keeps TestPubSub_EphemeralGroup (consumer test) from flaking
// under the H2 rejection fix.
func TestProduce_PubSubStaleResolverFreshCheck(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-h2-pubsub-stale-resolver")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	prod := testutil.StartProducer(t, ctx)

	// First produce: no groups → rejected. This primes the resolver's map
	// with an entry for this Pub/Sub queue, value []string{}.
	if _, err := prod.Produce(ctx, msg.New().SetBody("prime").SetQueue(params)); !errors.Is(err, publicproducer.ErrNoConsumerGroups) {
		t.Fatalf("prime produce: err = %v, want ErrNoConsumerGroups", err)
	}

	// Create a group directly via Redis SAdd — the resolver's event handler
	// may not have fired yet, so resolver.Resolve will still return
	// isPubSub=true, groups=[]. The fresh-check fallback must find the group
	// via SMEMBERS and produce successfully.
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	if err := redis.Client().SAdd(ctx, qKey.ConsumerGroups(), "fresh-group").Err(); err != nil {
		t.Fatalf("sadd group: %v", err)
	}

	ids, err := prod.Produce(ctx, msg.New().SetBody("should-succeed").SetQueue(params))
	if err != nil {
		t.Fatalf("produce after direct SAdd: %v (fresh-check fallback failed)", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d: %v", len(ids), ids)
	}
}
