/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Create a fanout exchange
func TestFanout_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := exchange.MustExchangeParams("test-fanout-create", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()

	err := fx.Create(ctx, exchangeParams, exchange.PolicyStandard)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
}

// Scenario: Bind queue to fanout exchange
func TestFanout_BindQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-fanout-bind-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-bind-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()

	err := fx.BindQueue(ctx, queueParams, exchangeParams)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
}

// Scenario: Fanout broadcasts to all bound queues
func TestFanout_BroadcastsToAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	q1 := queue.MustQueueParams("test-fanout-broadcast-q1")
	q2 := queue.MustQueueParams("test-fanout-broadcast-q2")
	q3 := queue.MustQueueParams("test-fanout-broadcast-q3")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q3, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-broadcast-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, q1, exchangeParams)
	fx.BindQueue(ctx, q2, exchangeParams)
	fx.BindQueue(ctx, q3, exchangeParams)

	var c1, c2, c3 atomic.Int64

	startConsumer(t, ctx, q1, &c1)
	startConsumer(t, ctx, q2, &c2)
	startConsumer(t, ctx, q3, &c3)

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("broadcast").
		SetFanoutExchange(exchangeParams),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 message IDs, got %d", len(ids))
	}

	time.Sleep(3 * time.Second)

	if c1.Load() != 1 {
		t.Errorf("q1: %d, want 1", c1.Load())
	}
	if c2.Load() != 1 {
		t.Errorf("q2: %d, want 1", c2.Load())
	}
	if c3.Load() != 1 {
		t.Errorf("q3: %d, want 1", c3.Load())
	}
}

// Scenario: Fanout ignores routing key
func TestFanout_IgnoresRoutingKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	q1 := queue.MustQueueParams("test-fanout-ignore-rk-q1")
	q2 := queue.MustQueueParams("test-fanout-ignore-rk-q2")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-ignore-rk-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, q1, exchangeParams)
	fx.BindQueue(ctx, q2, exchangeParams)

	var c1, c2 atomic.Int64
	startConsumer(t, ctx, q1, &c1)
	startConsumer(t, ctx, q2, &c2)

	prod := testutil.StartProducer(t, ctx)
	// Set routing key but fanout should ignore it and deliver to all
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("fanout").
		SetFanoutExchange(exchangeParams),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 message IDs, got %d", len(ids))
	}

	time.Sleep(3 * time.Second)

	if c1.Load() != 1 {
		t.Errorf("q1: %d, want 1", c1.Load())
	}
	if c2.Load() != 1 {
		t.Errorf("q2: %d, want 1", c2.Load())
	}
}

// Scenario: No routing key needed for fanout
func TestFanout_NoRoutingKeyNeeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	queueParams := queue.MustQueueParams("test-fanout-no-rk-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-no-rk-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, queueParams, exchangeParams)

	var count atomic.Int64
	startConsumer(t, ctx, queueParams, &count)

	prod := testutil.StartProducer(t, ctx)
	// No routing key set — fanout doesn't need one
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("fanout-no-rk").
		SetFanoutExchange(exchangeParams),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 message ID, got %d", len(ids))
	}

	time.Sleep(2 * time.Second)

	if count.Load() != 1 {
		t.Errorf("consumed: %d, want 1", count.Load())
	}
}

// Scenario: Unbind queue from fanout
func TestFanout_Unbind(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-fanout-unbind-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-unbind-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()

	fx.BindQueue(ctx, queueParams, exchangeParams)
	fx.UnbindQueue(ctx, queueParams, exchangeParams)

	queues, _ := fx.BoundQueues(ctx, exchangeParams)
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues after unbind, got %d", len(queues))
	}
}

// Scenario: List bound queues
func TestFanout_BoundQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := queue.MustQueueParams("test-fanout-bound-q1")
	q2 := queue.MustQueueParams("test-fanout-bound-q2")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-bound-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, q1, exchangeParams)
	fx.BindQueue(ctx, q2, exchangeParams)

	queues, err := fx.BoundQueues(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("bound queues: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("queues = %d, want 2", len(queues))
	}
}

// Scenario: Delete fanout exchange with bound queues fails
func TestFanout_DeleteWithBoundQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-fanout-delete-bound-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-delete-bound-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, queueParams, exchangeParams)

	err := fx.Delete(ctx, exchangeParams)
	if err == nil {
		t.Fatal("expected error: cannot delete exchange with bound queues")
	}
}

// Scenario: Delete fanout exchange after unbinding
func TestFanout_DeleteAfterUnbind(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-fanout-delete-unbind-q")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-delete-unbind-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, queueParams, exchangeParams)
	fx.UnbindQueue(ctx, queueParams, exchangeParams)

	err := fx.Delete(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestFanout_MatchQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := queue.MustQueueParams("test-fanout-match-q1")
	q2 := queue.MustQueueParams("test-fanout-match-q2")
	testutil.CreateQueue(t, ctx, q1, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-fanout-match-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.BindQueue(ctx, q1, exchangeParams)
	fx.BindQueue(ctx, q2, exchangeParams)

	queues, err := fx.MatchQueues(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("MatchQueues: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("MatchQueues returned %d queues, want 2", len(queues))
	}
}

// Helper
func startConsumer(t *testing.T, ctx context.Context, params *queue.Params, counter *atomic.Int64) {
	t.Helper()
	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error {
		counter.Add(1)
		return nil
	})
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	t.Cleanup(func() { cons.Shutdown() })
}
