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
	"errors"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Standard policy allows FIFO queue
func TestPolicy_StandardAllowsFIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-standard-fifo")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-standard-fifo-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("FIFO should be allowed with standard policy: %v", err)
	}
}

// Scenario: Standard policy allows LIFO queue
func TestPolicy_StandardAllowsLIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-standard-lifo")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeLIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-standard-lifo-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("LIFO should be allowed with standard policy: %v", err)
	}
}

// Scenario: Standard policy rejects Priority queue
func TestPolicy_StandardRejectsPriority(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-standard-prio")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypePriority, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-standard-prio-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("Priority queue should be rejected with standard policy")
	}
}

// Scenario: Priority policy allows Priority queue
func TestPolicy_PriorityAllowsPriority(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-prio-allows")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypePriority, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-prio-allows-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("Priority queue should be allowed with priority policy: %v", err)
	}
}

// Scenario: Priority policy rejects FIFO queue
func TestPolicy_PriorityRejectsFIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-prio-rejects-fifo")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-prio-rejects-fifo-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("FIFO queue should be rejected with priority policy")
	}
}

// Scenario: Priority policy rejects LIFO queue
func TestPolicy_PriorityRejectsLIFO(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-prio-rejects-lifo")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeLIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-prio-rejects-lifo-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	dx.Create(ctx, exchangeParams, exchange.PolicyPriority)

	err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("LIFO queue should be rejected with priority policy")
	}
}

// Scenario: Policy is enforced on topic exchanges
func TestPolicy_TopicExchangeEnforcement(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-topic-prio")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypePriority, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-topic-prio-ex", exchange.TypeTopic)
	tx := redissmq.NewTopicExchange()
	tx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := tx.BindQueue(ctx, queueParams, exchangeParams, "test.*")
	if err == nil {
		t.Fatal("Priority queue should be rejected on standard topic exchange")
	}
}

// Scenario: Policy is enforced on fanout exchanges
func TestPolicy_FanoutExchangeEnforcement(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-policy-fanout-prio")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypePriority, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-fanout-prio-ex", exchange.TypeFanout)
	fx := redissmq.NewFanoutExchange()
	fx.Create(ctx, exchangeParams, exchange.PolicyStandard)

	err := fx.BindQueue(ctx, queueParams, exchangeParams)
	if err == nil {
		t.Fatal("Priority queue should be rejected on standard fanout exchange")
	}
}

// Scenario: FIFO-first bind auto-creates an exchange with PolicyStandard;
// a subsequent priority bind is rejected with ErrPolicyViolation.
//
// Companion test TestPolicy_AutoCreatedExchangePriorityFirst covers the
// priority-first case. Pre-fix, priority-first auto-created the exchange
// with the zero-value PolicyStandard — causing all subsequent priority
// binds to fail and FIFO binds to incorrectly succeed against what
// should be a priority exchange.
func TestPolicy_AutoCreatedExchange(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQueue := queue.MustQueueParams("test-policy-auto-fifo")
	prioQueue := queue.MustQueueParams("test-policy-auto-prio")
	testutil.CreateQueue(t, ctx, fifoQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, prioQueue, queue.TypePriority, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-auto-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	// First bind auto-creates exchange — FIFO queue → PolicyStandard
	err := dx.BindQueue(ctx, fifoQueue, exchangeParams, "test.key")
	if err != nil {
		t.Fatalf("first bind should succeed: %v", err)
	}

	// Now exchange exists with PolicyStandard — priority rejected
	err = dx.BindQueue(ctx, prioQueue, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("priority queue should be rejected after exchange was created")
	}
}

// Scenario: A priority queue as the first binder auto-creates the
// exchange with PolicyPriority (derived from the queue type). Subsequent
// priority binds succeed; a subsequent FIFO bind fails.
//
// Regression test for the pre-fix behaviour where auto-create wrote only
// the type field — policy defaulted to PolicyStandard — so a priority-
// first bind produced a standard-policy exchange. Second priority binds
// then failed with ErrPolicyViolation (impossible state: two priority
// queues can't share an exchange), and FIFO binds incorrectly succeeded
// against what should have been a priority exchange.
func TestPolicy_AutoCreatedExchangePriorityFirst(t *testing.T) {
	ctx := testutil.Setup(t)

	prio1 := queue.MustQueueParams("test-policy-auto-prio-first-1")
	prio2 := queue.MustQueueParams("test-policy-auto-prio-first-2")
	fifo := queue.MustQueueParams("test-policy-auto-prio-first-fifo")
	testutil.CreateQueue(t, ctx, prio1, queue.TypePriority, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, prio2, queue.TypePriority, queue.DeliveryPointToPoint)
	testutil.CreateQueue(t, ctx, fifo, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-policy-auto-prio-first-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()

	// First bind: priority queue auto-creates the exchange.
	if err := dx.BindQueue(ctx, prio1, exchangeParams, "test.key"); err != nil {
		t.Fatalf("first priority bind (auto-create): %v", err)
	}

	// Verify the auto-created policy is PolicyPriority, not PolicyStandard.
	em := redissmq.NewExchangeManager()
	props, err := em.Properties(ctx, exchangeParams)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.Policy != exchange.PolicyPriority {
		t.Fatalf("auto-created policy = %v, want PolicyPriority "+
			"(pre-fix: PolicyStandard, making the exchange unbindable by other priority queues)",
			props.Policy)
	}

	// Second priority bind: must succeed (same policy).
	if err := dx.BindQueue(ctx, prio2, exchangeParams, "test.key"); err != nil {
		t.Fatalf("second priority bind: %v (pre-fix: ErrPolicyViolation)", err)
	}

	// FIFO bind: must fail (policy mismatch).
	err = dx.BindQueue(ctx, fifo, exchangeParams, "test.key")
	if err == nil {
		t.Fatal("FIFO queue should be rejected on priority exchange " +
			"(pre-fix: incorrectly succeeded because exchange was PolicyStandard)")
	}
	if !errors.Is(err, exchange.ErrPolicyViolation) {
		t.Errorf("err = %v, want ErrPolicyViolation", err)
	}
}
