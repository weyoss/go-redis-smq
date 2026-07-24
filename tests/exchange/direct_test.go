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
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Create a direct exchange
func TestDirect_Create(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-direct-create", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	err := dx.Create(ctx, params, x.PolicyStandard)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
}

// Scenario: Bind queue to a direct exchange routing key
func TestDirect_BindQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-direct-bind-queue")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-bind-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "order.created")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
}

// Scenario: Bind auto-creates exchange
func TestDirect_BindAutoCreatesExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-direct-auto-create-q")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-auto-create-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	// Bind without creating first — should auto-create
	err := dx.BindQueue(ctx, queueParams, exchangeParams, "order.created")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Verify exchange exists
	em := exchange.NewManager()
	exists, err := em.Exists(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("exchange should exist after bind")
	}
}

// Scenario: Match queues for a routing key
func TestDirect_MatchQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParams("test-direct-match-q1")
	q2 := q.MustQueueParams("test-direct-match-q2")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-match-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	dx.BindQueue(ctx, q1, exchangeParams, "order.created")
	dx.BindQueue(ctx, q2, exchangeParams, "order.created")

	queues, err := dx.MatchQueues(ctx, exchangeParams, "order.created")
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("matched %d queues, want 2", len(queues))
	}
}

// Scenario: Routing key with no bindings returns empty
func TestDirect_NoMatch(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := x.MustExchangeParams("test-direct-nomatch", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	queues, err := dx.MatchQueues(ctx, exchangeParams, "nonexistent.key")
	if err != nil {
		t.Fatalf("match: %v", err)
	}
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues, got %d", len(queues))
	}
}

// Scenario: Unbind queue from routing key
func TestDirect_UnbindQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queue := q.MustQueueParams("test-direct-unbind-q")
	testutil.CreateQueue(t, ctx, queue, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-unbind-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	dx.BindQueue(ctx, queue, exchangeParams, "order.created")

	err := dx.UnbindQueue(ctx, queue, exchangeParams, "order.created")
	if err != nil {
		t.Fatalf("unbind: %v", err)
	}

	queues, _ := dx.MatchQueues(ctx, exchangeParams, "order.created")
	if len(queues) != 0 {
		t.Fatalf("expected 0 queues after unbind, got %d", len(queues))
	}
}

// Scenario: List routing keys
func TestDirect_RoutingKeys(t *testing.T) {
	ctx := testutil.Setup(t)

	exchangeParams := x.MustExchangeParams("test-direct-keys-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	queue := q.MustQueueParams("test-direct-keys-q")
	testutil.CreateQueue(t, ctx, queue, q.TypeFIFO, q.DeliveryPointToPoint)

	dx.BindQueue(ctx, queue, exchangeParams, "order.created")
	dx.BindQueue(ctx, queue, exchangeParams, "order.cancelled")

	keys, err := dx.RoutingKeys(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("routing keys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys = %d, want 2", len(keys))
	}
}

// Scenario: Produce and consume via direct exchange
func TestDirect_ProduceConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(testutil.Setup(t), 10*time.Second)
	defer cancel()

	q1 := q.MustQueueParams("test-direct-prod-consume-q1")
	q2 := q.MustQueueParams("test-direct-prod-consume-q2")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-prod-consume-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.BindQueue(ctx, q1, exchangeParams, "order.created")
	dx.BindQueue(ctx, q2, exchangeParams, "order.created")

	var c1, c2 atomic.Int64

	cons1 := redissmq.NewConsumer()
	cons1.Consume(q1, func(ctx context.Context, m *msg.Transferable) error {
		c1.Add(1)
		return nil
	})
	cons1.Run(ctx)
	defer cons1.Shutdown()

	cons2 := redissmq.NewConsumer()
	cons2.Consume(q2, func(ctx context.Context, m *msg.Transferable) error {
		c2.Add(1)
		return nil
	})
	cons2.Run(ctx)
	defer cons2.Shutdown()

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().
		SetBody("order").
		SetDirectExchange(exchangeParams).
		SetExchangeRoutingKey("order.created"),
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 message IDs, got %d", len(ids))
	}

	time.Sleep(3 * time.Second)

	if c1.Load() != 1 {
		t.Errorf("q1: %d messages, want 1", c1.Load())
	}
	if c2.Load() != 1 {
		t.Errorf("q2: %d messages, want 1", c2.Load())
	}
}

// Scenario: Duplicate binding returns error
func TestDirect_DuplicateBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	queue := q.MustQueueParams("test-direct-dup-bind-q")
	testutil.CreateQueue(t, ctx, queue, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-dup-bind-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	dx.BindQueue(ctx, queue, exchangeParams, "order.created")

	err := dx.BindQueue(ctx, queue, exchangeParams, "order.created")
	if err == nil {
		t.Fatal("expected error for duplicate binding")
	}
}

// Scenario: Delete exchange with bound queues returns error
func TestDirect_DeleteWithBoundQueues(t *testing.T) {
	ctx := testutil.Setup(t)

	queue := q.MustQueueParams("test-direct-delete-bound-q")
	testutil.CreateQueue(t, ctx, queue, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-delete-bound-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.BindQueue(ctx, queue, exchangeParams, "order.created")

	err := dx.Delete(ctx, exchangeParams)
	if err == nil {
		t.Fatal("expected error: cannot delete exchange with bound queues")
	}
}

// Scenario: Delete exchange after unbinding all queues
func TestDirect_DeleteAfterUnbind(t *testing.T) {
	ctx := testutil.Setup(t)

	queue := q.MustQueueParams("test-direct-delete-unbind-q")
	testutil.CreateQueue(t, ctx, queue, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-delete-unbind-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)
	dx.BindQueue(ctx, queue, exchangeParams, "order.created")
	dx.UnbindQueue(ctx, queue, exchangeParams, "order.created")

	err := dx.Delete(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	em := exchange.NewManager()
	exists, _ := em.Exists(ctx, exchangeParams)
	if exists {
		t.Fatal("exchange should not exist after delete")
	}
}

// Scenario: Multiple queues on different routing keys
func TestDirect_MultipleRoutingKeys(t *testing.T) {
	ctx := testutil.Setup(t)

	q1 := q.MustQueueParams("test-direct-multi-rk-q1")
	q2 := q.MustQueueParams("test-direct-multi-rk-q2")
	testutil.CreateQueue(t, ctx, q1, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, q2, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-direct-multi-rk-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange(nil)

	dx.BindQueue(ctx, q1, exchangeParams, "order.created")
	dx.BindQueue(ctx, q2, exchangeParams, "order.cancelled")

	queues1, _ := dx.MatchQueues(ctx, exchangeParams, "order.created")
	queues2, _ := dx.MatchQueues(ctx, exchangeParams, "order.cancelled")

	if len(queues1) != 1 || queues1[0].String() != q1.String() {
		t.Errorf("order.created should match q1 only, got %v", queues1)
	}
	if len(queues2) != 1 || queues2[0].String() != q2.String() {
		t.Errorf("order.cancelled should match q2 only, got %v", queues2)
	}
}
