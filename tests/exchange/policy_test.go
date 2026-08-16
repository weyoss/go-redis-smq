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
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Standard policy allows FIFO queue
func TestPolicy_StandardAllowsFIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-standard-fifo")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-standard-fifo-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("FIFO should be allowed with standard policy: %v", err)
	}
}

// Scenario: Standard policy allows LIFO queue
func TestPolicy_StandardAllowsLIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-standard-lifo")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeLIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-standard-lifo-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("LIFO should be allowed with standard policy: %v", err)
	}
}

// Scenario: Standard policy rejects Priority queue
func TestPolicy_StandardRejectsPriority(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-standard-prio")
	testutil.CreateQueue(t, ctx, queueParams, q.TypePriority, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-standard-prio-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("Priority queue should be rejected with standard policy")
	}
}

// Scenario: Priority policy allows Priority queue
func TestPolicy_PriorityAllowsPriority(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-prio-allows")
	testutil.CreateQueue(t, ctx, queueParams, q.TypePriority, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-prio-allows-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("Priority queue should be allowed with priority policy: %v", err)
	}
}

// Scenario: Priority policy rejects FIFO queue
func TestPolicy_PriorityRejectsFIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-prio-rejects-fifo")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-prio-rejects-fifo-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("FIFO queue should be rejected with priority policy")
	}
}

// Scenario: Priority policy rejects LIFO queue
func TestPolicy_PriorityRejectsLIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-prio-rejects-lifo")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeLIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-prio-rejects-lifo-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	dx.Create(ctx, exchangeParams, x.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("LIFO queue should be rejected with priority policy")
	}
}

// Scenario: Policy is enforced on topic exchanges
func TestPolicy_TopicExchangeEnforcement(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-topic-prio")
	testutil.CreateQueue(t, ctx, queueParams, q.TypePriority, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-topic-prio-ex", x.TypeTopic)
	tx := exchange.NewTopicExchange()
	tx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := tx.BindQueue(ctx, queueParams, exchangeParams, "test.*")
	if err == nil {
		t.Fatal("Priority queue should be rejected on standard topic exchange")
	}
}

// Scenario: Policy is enforced on fanout exchanges
func TestPolicy_FanoutExchangeEnforcement(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := q.MustQueueParams("test-policy-fanout-prio")
	testutil.CreateQueue(t, ctx, queueParams, q.TypePriority, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-fanout-prio-ex", x.TypeFanout)
	fx := exchange.NewFanoutExchange()
	fx.Create(ctx, exchangeParams, x.PolicyStandard)

	err := fx.BindQueue(ctx, queueParams, exchangeParams)
	if err == nil {
		t.Fatal("Priority queue should be rejected on standard fanout exchange")
	}
}

// Scenario: Policy validation on auto-created exchange
func TestPolicy_AutoCreatedExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQueue := q.MustQueueParams("test-policy-auto-fifo")
	prioQueue := q.MustQueueParams("test-policy-auto-prio")
	testutil.CreateQueue(t, ctx, fifoQueue, q.TypeFIFO, q.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, prioQueue, q.TypePriority, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-policy-auto-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()

	// First bind auto-creates exchange — no policy set yet, FIFO allowed
	err := dx.BindQueue(ctx, fifoQueue, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("first bind should succeed: %v", err)
	}

	// Now exchange exists with FIFO queue — priority should still be rejected
	// because the exchange type was set but policy defaults to standard
	err = dx.BindQueue(ctx, prioQueue, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("priority queue should be rejected after exchange was created")
	}
}
