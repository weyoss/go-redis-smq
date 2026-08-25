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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Manager.Create and Manager.Properties
func TestManager_CreateAndProperties(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-manager-create", x.TypeDirect)
	em := exchange.NewManager()

	if err := em.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := em.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.Type != x.TypeDirect {
		t.Errorf("type = %v, want direct", props.Type)
	}
	if props.Policy != x.PolicyStandard {
		t.Errorf("policy = %v, want standard", props.Policy)
	}
}

// Scenario: Manager.Exists
func TestManager_Exists(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-manager-exists", x.TypeFanout)
	em := exchange.NewManager()

	exists, err := em.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists (before create): %v", err)
	}
	if exists {
		t.Fatal("exchange should not exist before creation")
	}

	if err := em.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err = em.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists (after create): %v", err)
	}
	if !exists {
		t.Fatal("exchange should exist after creation")
	}
}

// Scenario: Manager.ValidateType – matching and mismatching types
func TestManager_ValidateType(t *testing.T) {
	ctx := testutil.Setup(t)

	directParams := x.MustExchangeParams("test-manager-validate-type", x.TypeDirect)
	em := exchange.NewManager()

	// required=false should not error even if missing
	if err := em.ValidateType(ctx, directParams, false); err != nil {
		t.Fatalf("validate type (missing, required=false): %v", err)
	}

	// required=true should error when missing
	if err := em.ValidateType(ctx, directParams, true); !errors.Is(err, x.ErrNotFound) {
		t.Fatalf("validate type (missing, required=true): got %v, want ErrNotFound", err)
	}

	// Create as direct
	if err := em.Create(ctx, directParams, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Same type should succeed
	if err := em.ValidateType(ctx, directParams, true); err != nil {
		t.Fatalf("validate type (matching): %v", err)
	}

	// Different type should return TypeMismatchError
	topicParams := x.MustExchangeParams("test-manager-validate-type", x.TypeTopic)
	err := em.ValidateType(ctx, topicParams, true)
	var typeErr *x.TypeMismatchError
	if !errors.As(err, &typeErr) {
		t.Fatalf("validate type mismatch: got %v, want TypeMismatchError", err)
	}
}

// Scenario: Manager.ValidateBinding – missing exchange, valid binding, policy violation
func TestManager_ValidateBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQueue := queue.MustQueueParams("test-manager-binding-fifo")
	testutil.CreateQueue(t, ctx, fifoQueue, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prioQueue := queue.MustQueueParams("test-manager-binding-prio")
	testutil.CreateQueue(t, ctx, prioQueue, queue.TypePriority, queue.DeliveryPointToPoint)

	directParams := x.MustExchangeParams("test-manager-binding-ex", x.TypeDirect)
	em := exchange.NewManager()

	// Exchange doesn't exist yet → nil, nil
	props, err := em.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (missing exchange): %v", err)
	}
	if props != nil {
		t.Fatal("expected nil properties for missing exchange")
	}

	// Create standard direct exchange
	if err := em.Create(ctx, directParams, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Valid FIFO queue binding
	props, err = em.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (fifo): %v", err)
	}
	if props == nil {
		t.Fatal("expected exchange properties")
	}
	if props.Type != x.TypeDirect {
		t.Errorf("type = %v, want direct", props.Type)
	}

	// Policy violation – Priority queue cannot bind to Standard exchange
	_, err = em.ValidateBinding(ctx, directParams, prioQueue)
	var policyErr *x.PolicyViolationError
	if !errors.As(err, &policyErr) {
		t.Fatalf("validate binding (priority): got %v, want PolicyViolationError", err)
	}
}

// Scenario: Manager.Delete
func TestManager_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-manager-delete", x.TypeTopic)
	em := exchange.NewManager()

	if err := em.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := em.Delete(ctx, params); err != nil {
		t.Fatalf("delete: %v", err)
	}

	exists, err := em.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists after delete: %v", err)
	}
	if exists {
		t.Fatal("exchange should not exist after delete")
	}
}

// Scenario: Manager.ListByQueue
func TestManager_ListByQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	queueParams := queue.MustQueueParams("test-manager-list-by-queue")
	testutil.CreateQueue(t, ctx, queueParams, queue.TypeFIFO, queue.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-manager-list-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	if err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key"); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	em := exchange.NewManager()
	exchanges, err := em.ListByQueue(ctx, queueParams)
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

// Scenario: Manager.ListAll
func TestManager_ListAll(t *testing.T) {
	ctx := testutil.Setup(t)

	em := exchange.NewManager()

	ex1 := x.MustExchangeParamsWithNS("test-manager-list-all-1", "ns-a", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-manager-list-all-2", "ns-b", x.TypeFanout)
	if err := em.Create(ctx, ex1, x.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := em.Create(ctx, ex2, x.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	all, err := em.ListAll(ctx)
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

// Scenario: Manager.ListByNamespace
func TestManager_ListByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	em := exchange.NewManager()

	ex1 := x.MustExchangeParamsWithNS("test-manager-list-ns-1", "ns-list", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-manager-list-ns-2", "ns-list", x.TypeTopic)
	if err := em.Create(ctx, ex1, x.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := em.Create(ctx, ex2, x.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	exchanges, err := em.ListByNamespace(ctx, "ns-list")
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
