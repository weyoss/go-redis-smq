/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicConsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: consumer message events must carry the effective consumer
// group ID in the queue payload, matching TypeScript's IQueueParsedParams
// wire format. Pre-fix, Go serialized the queue as {name, ns} only —
// TypeScript subscribers reading `queue.queueParams.name` got undefined,
// and `queue.groupId` was always missing.
func TestConsumerEvents_CarryConsumerGroupID(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-h1-consumer-group-id")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPubSub)

	const groupID = "email-service"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("create group: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	var received publicConsumer.MessagePayload
	sub, err := publicConsumer.SubscribeMessageAcknowledged(func(p publicConsumer.MessagePayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	prod := testutil.StartProducer(t, ctx)
	if _, err := prod.Produce(ctx, msg.New().SetBody("h1").SetQueue(params)); err != nil {
		t.Fatalf("produce: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for acknowledged event")
	}

	// Queue.QueueParams must be populated (the {name, ns} inner shape).
	if received.Queue.QueueParams.Name() != params.Name() {
		t.Errorf("queueParams.name = %q, want %q (H1: queueParams not nested under Queue.QueueParams)",
			received.Queue.QueueParams.Name(), params.Name())
	}
	if received.Queue.QueueParams.NS() != params.NS() {
		t.Errorf("queueParams.ns = %q, want %q", received.Queue.QueueParams.NS(), params.NS())
	}

	// Queue.GroupID must be the effective group ID — this is the H1 fix.
	if received.Queue.GroupID != groupID {
		t.Errorf("groupId = %q, want %q (H1: groupId not threaded into event payload)",
			received.Queue.GroupID, groupID)
	}
}
