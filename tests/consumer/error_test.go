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
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Consumer run with no handlers returns error
func TestError_NoHandlers(t *testing.T) {
	ctx := testutil.Setup(t)

	cons := redissmq.NewConsumer()
	err := cons.Run(ctx)
	if err == nil {
		t.Fatal("expected error: no queues registered")
	}
}

// Scenario: Consumer cannot consume from non-existent queue
func TestError_ConsumeNonExistentQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("nonexistent")

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})
	err := cons.Run(ctx)
	t.Logf("run error: %v", err)
	cons.Shutdown()
}

// Scenario: Consumer cannot consume from stopped queue
func TestError_ConsumeStoppedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-stopped")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	queue.Stop(ctx, params, nil)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})

	err := cons.Run(ctx)
	t.Logf("run on stopped queue: %v", err)
	cons.Shutdown()
}

// Scenario: Consumer handles queue state change to stopped
func TestError_QueueStoppedDuringConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	params := q.MustQueueParams("test-error-stop-during")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	for i := 0; i < 10; i++ {
		prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	}

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		time.Sleep(5 * time.Second)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	defer cons.Shutdown()

	time.Sleep(1 * time.Second)

	queue.Stop(ctx, params, nil)

	time.Sleep(2 * time.Second)

	if !cons.IsRunning() {
		t.Log("consumer stopped after queue was stopped")
	}
}

// Scenario: Consumer run is idempotent
func TestError_RunTwice(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-run-twice")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		return nil
	})

	err := cons.Run(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	err = cons.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	cons.Shutdown()
}
