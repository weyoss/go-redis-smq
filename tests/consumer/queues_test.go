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
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Queues() must return the effective consumer group ID for
// Pub/Sub handlers.
func TestQueues_PointToPoint_EmptyGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-2-2-queues-p2p")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	queues := cons.Queues()
	if len(queues) != 1 {
		t.Fatalf("Queues() = %d entries, want 1", len(queues))
	}

	q := queues[0]
	if q.QueueParams.Name() != params.Name() || q.QueueParams.NS() != params.NS() {
		t.Errorf("QueueParams = %q@%q, want %q@%q",
			q.QueueParams.Name(), q.QueueParams.NS(),
			params.Name(), params.NS())
	}
	if q.GroupID != "" {
		t.Errorf("GroupID = %q, want empty for point-to-point queue", q.GroupID)
	}
}

func TestQueues_PubSub_ExplicitGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-2-2-queues-pubsub-explicit")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	const groupID = "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("create group: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	queues := cons.Queues()
	if len(queues) != 1 {
		t.Fatalf("Queues() = %d entries, want 1", len(queues))
	}

	q := queues[0]
	if q.QueueParams.Name() != params.Name() {
		t.Errorf("QueueParams.Name() = %q, want %q", q.QueueParams.Name(), params.Name())
	}
	if q.GroupID != groupID {
		t.Errorf("GroupID = %q, want %q (explicit group)", q.GroupID, groupID)
	}
}

func TestQueues_PubSub_EphemeralGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-2-2-queues-pubsub-ephemeral")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	queues := cons.Queues()
	if len(queues) != 1 {
		t.Fatalf("Queues() = %d entries, want 1", len(queues))
	}

	q := queues[0]
	if q.QueueParams.Name() != params.Name() {
		t.Errorf("QueueParams.Name() = %q, want %q", q.QueueParams.Name(), params.Name())
	}
	// Ephemeral group format: "cid-" + consumerID
	expectedGroupID := "cid-" + cons.ID()
	if q.GroupID != expectedGroupID {
		t.Errorf("GroupID = %q, want %q (ephemeral group)", q.GroupID, expectedGroupID)
	}
}

// Scenario: multiple handlers with different (queue, group) pairs must
// all appear in Queues(), each with its own effective group ID.
func TestQueues_MultipleHandlers(t *testing.T) {
	ctx := testutil.Setup(t)

	p2pParams := publicqueue.MustQueueParams("test-2-2-queues-multi-p2p")
	testutil.CreateQueue(t, ctx, p2pParams, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	pubsubParams := publicqueue.MustQueueParams("test-2-2-queues-multi-pubsub")
	testutil.CreateQueue(t, ctx, pubsubParams, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	const groupID = "workers"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, pubsubParams, groupID); err != nil {
		t.Fatalf("create group: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.Consume(p2pParams, func(ctx context.Context, m *msg.Transferable) error { return nil })
	cons.ConsumeWithGroup(pubsubParams, groupID, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	queues := cons.Queues()
	if len(queues) != 2 {
		t.Fatalf("Queues() = %d entries, want 2", len(queues))
	}

	byQueue := make(map[string]publicqueue.ParsedParams, len(queues))
	for _, q := range queues {
		byQueue[q.QueueParams.Name()] = q
	}

	p2p, ok := byQueue[p2pParams.Name()]
	if !ok {
		t.Fatalf("missing p2p queue in Queues(): %v", queues)
	}
	if p2p.GroupID != "" {
		t.Errorf("p2p GroupID = %q, want empty", p2p.GroupID)
	}

	pubsub, ok := byQueue[pubsubParams.Name()]
	if !ok {
		t.Fatalf("missing pubsub queue in Queues(): %v", queues)
	}
	if pubsub.GroupID != groupID {
		t.Errorf("pubsub GroupID = %q, want %q", pubsub.GroupID, groupID)
	}
}
