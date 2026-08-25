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

// Scenario: Package-level Create and Properties
func TestPackage_CreateAndProperties(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("test-package-create", exchange.TypeDirect)
	xm := redissmq.NewExchangeManager()
	if err := xm.Create(ctx, params, exchange.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := xm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.Type != exchange.TypeDirect {
		t.Errorf("type = %v, want direct", props.Type)
	}
	if props.Policy != exchange.PolicyStandard {
		t.Errorf("policy = %v, want standard", props.Policy)
	}
}

// Scenario: Package-level Exists
func TestPackage_Exists(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("test-package-exists", exchange.TypeFanout)

	xm := redissmq.NewExchangeManager()
	exists, err := xm.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists (before create): %v", err)
	}
	if exists {
		t.Fatal("exchange should not exist before creation")
	}

	if err := xm.Create(ctx, params, exchange.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err = xm.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists (after create): %v", err)
	}
	if !exists {
		t.Fatal("exchange should exist after creation")
	}
}

// Scenario: Package-level ValidateType
func TestPackage_ValidateType(t *testing.T) {
	ctx := testutil.Setup(t)

	directParams := exchange.MustExchangeParams("test-package-validate-type", exchange.TypeDirect)
	xm := redissmq.NewExchangeManager()

	// required=false should not error even if missing
	if err := xm.ValidateType(ctx, directParams, false); err != nil {
		t.Fatalf("validate type (missing, required=false): %v", err)
	}

	// required=true should error when missing
	if err := xm.ValidateType(ctx, directParams, true); !errors.Is(err, exchange.ErrNotFound) {
		t.Fatalf("validate type (missing, required=true): got %v, want ErrNotFound", err)
	}

	// Create as direct
	if err := xm.Create(ctx, directParams, exchange.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Same type should succeed
	if err := xm.ValidateType(ctx, directParams, true); err != nil {
		t.Fatalf("validate type (matching): %v", err)
	}

	// Different type should return TypeMismatchError
	topicParams := exchange.MustExchangeParams("test-package-validate-type", exchange.TypeTopic)
	err := xm.ValidateType(ctx, topicParams, true)
	if !errors.Is(err, exchange.ErrTypeMismatch) {
		t.Fatalf("validate type mismatch: got %v, want ErrTypeMismatch", err)
	}
}

// Scenario: Package-level ValidateBinding
func TestPackage_ValidateBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQueue := queue.MustQueueParams("test-package-binding-fifo")
	testutil.CreateQueue(t, ctx, fifoQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prioQueue := queue.MustQueueParams("test-package-binding-prio")
	testutil.CreateQueue(t, ctx, prioQueue, queue.TypePriority, queue.DeliveryPointToPoint)

	directParams := exchange.MustExchangeParams("test-package-binding-ex", exchange.TypeDirect)

	// Exchange doesn't exist yet → nil, nil
	xm := redissmq.NewExchangeManager()
	props, err := xm.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (missing exchange): %v", err)
	}
	if props != nil {
		t.Fatal("expected nil properties for missing exchange")
	}

	// Create standard direct exchange
	if err := xm.Create(ctx, directParams, exchange.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Valid FIFO queue binding
	props, err = xm.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (fifo): %v", err)
	}
	if props == nil {
		t.Fatal("expected exchange properties")
	}

	// Policy violation – Priority queue cannot bind to Standard exchange
	_, err = xm.ValidateBinding(ctx, directParams, prioQueue)
	if !errors.Is(err, exchange.ErrPolicyViolation) {
		t.Fatalf("validate binding (priority): got %v, want PolicyViolationError", err)
	}
}

// Scenario: Package-level Delete
func TestPackage_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := exchange.MustExchangeParams("test-package-delete", exchange.TypeTopic)
	xm := redissmq.NewExchangeManager()
	if err := xm.Create(ctx, params, exchange.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := xm.Delete(ctx, params); err != nil {
		t.Fatalf("delete: %v", err)
	}

	exists, err := xm.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists after delete: %v", err)
	}
	if exists {
		t.Fatal("exchange should not exist after delete")
	}
}

// Scenario: Package-level ListByQueue
func TestPackage_ListByQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-package-list-by-queue")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := exchange.MustExchangeParams("test-package-list-ex", exchange.TypeDirect)
	dx := redissmq.NewDirectExchange()
	if err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key"); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	xm := redissmq.NewExchangeManager()
	exchanges, err := xm.ListByQueue(ctx, queueParams)
	if err != nil {
		t.Fatalf("list by queue: %v", err)
	}
	if len(exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(exchanges))
	}
	if exchanges[0].String() != exchangeParams.String() {
		t.Errorf("exchange = %s, want %s", exchanges[0].String(), exchangeParams.String())
	}
}

// Scenario: Package-level ListAll
func TestPackage_ListAll(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := exchange.MustExchangeParamsWithNS("test-package-list-all-1", "ns-package-a", exchange.TypeDirect)
	ex2 := exchange.MustExchangeParamsWithNS("test-package-list-all-2", "ns-package-b", exchange.TypeFanout)
	xm := redissmq.NewExchangeManager()
	if err := xm.Create(ctx, ex1, exchange.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := xm.Create(ctx, ex2, exchange.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	all, err := xm.ListAll(ctx)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}

	found := make(map[string]bool)
	for _, e := range all {
		found[e.String()] = true
	}
	if !found[ex1.String()] {
		t.Errorf("missing exchange: %s", ex1.String())
	}
	if !found[ex2.String()] {
		t.Errorf("missing exchange: %s", ex2.String())
	}
}

// Scenario: Package-level ListByNamespace
func TestPackage_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	ex1 := exchange.MustExchangeParamsWithNS("test-package-list-ns-1", "ns-package-list", exchange.TypeDirect)
	ex2 := exchange.MustExchangeParamsWithNS("test-package-list-ns-2", "ns-package-list", exchange.TypeTopic)
	xm := redissmq.NewExchangeManager()
	if err := xm.Create(ctx, ex1, exchange.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := xm.Create(ctx, ex2, exchange.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	exchanges, err := xm.ListByNamespace(ctx, "ns-package-list")
	if err != nil {
		t.Fatalf("list by namespace: %v", err)
	}

	found := make(map[string]bool)
	for _, e := range exchanges {
		found[e.String()] = true
	}
	if !found[ex1.String()] {
		t.Errorf("missing exchange: %s", ex1.String())
	}
	if !found[ex2.String()] {
		t.Errorf("missing exchange: %s", ex2.String())
	}
}
