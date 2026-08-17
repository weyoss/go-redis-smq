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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Package-level Create and Properties
func TestPackage_CreateAndProperties(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-package-create", x.TypeDirect)
	if err := exchange.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	props, err := exchange.Properties(ctx, params)
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

// Scenario: Package-level Exists
func TestPackage_Exists(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-package-exists", x.TypeFanout)

	exists, err := exchange.Exists(ctx, params)
	if err != nil {
		t.Fatalf("exists (before create): %v", err)
	}
	if exists {
		t.Fatal("exchange should not exist before creation")
	}

	if err := exchange.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	exists, err = exchange.Exists(ctx, params)
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

	directParams := x.MustExchangeParams("test-package-validate-type", x.TypeDirect)

	// required=false should not error even if missing
	if err := exchange.ValidateType(ctx, directParams, false); err != nil {
		t.Fatalf("validate type (missing, required=false): %v", err)
	}

	// required=true should error when missing
	if err := exchange.ValidateType(ctx, directParams, true); !errors.Is(err, x.ErrNotFound) {
		t.Fatalf("validate type (missing, required=true): got %v, want ErrNotFound", err)
	}

	// Create as direct
	if err := exchange.Create(ctx, directParams, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Same type should succeed
	if err := exchange.ValidateType(ctx, directParams, true); err != nil {
		t.Fatalf("validate type (matching): %v", err)
	}

	// Different type should return TypeMismatchError
	topicParams := x.MustExchangeParams("test-package-validate-type", x.TypeTopic)
	err := exchange.ValidateType(ctx, topicParams, true)
	var typeErr *x.TypeMismatchError
	if !errors.As(err, &typeErr) {
		t.Fatalf("validate type mismatch: got %v, want TypeMismatchError", err)
	}
}

// Scenario: Package-level ValidateBinding
func TestPackage_ValidateBinding(t *testing.T) {
	ctx := testutil.Setup(t)

	fifoQueue := q.MustQueueParams("test-package-binding-fifo")
	testutil.CreateQueue(t, ctx, fifoQueue, q.TypeFIFO, q.DeliveryPointToPoint)

	prioQueue := q.MustQueueParams("test-package-binding-prio")
	testutil.CreateQueue(t, ctx, prioQueue, q.TypePriority, q.DeliveryPointToPoint)

	directParams := x.MustExchangeParams("test-package-binding-ex", x.TypeDirect)

	// Exchange doesn't exist yet → nil, nil
	props, err := exchange.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (missing exchange): %v", err)
	}
	if props != nil {
		t.Fatal("expected nil properties for missing exchange")
	}

	// Create standard direct exchange
	if err := exchange.Create(ctx, directParams, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Valid FIFO queue binding
	props, err = exchange.ValidateBinding(ctx, directParams, fifoQueue)
	if err != nil {
		t.Fatalf("validate binding (fifo): %v", err)
	}
	if props == nil {
		t.Fatal("expected exchange properties")
	}

	// Policy violation – Priority queue cannot bind to Standard exchange
	_, err = exchange.ValidateBinding(ctx, directParams, prioQueue)
	var policyErr *x.PolicyViolationError
	if !errors.As(err, &policyErr) {
		t.Fatalf("validate binding (priority): got %v, want PolicyViolationError", err)
	}
}

// Scenario: Package-level Delete
func TestPackage_Delete(t *testing.T) {
	ctx := testutil.Setup(t)

	params := x.MustExchangeParams("test-package-delete", x.TypeTopic)
	if err := exchange.Create(ctx, params, x.PolicyStandard); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := exchange.Delete(ctx, params); err != nil {
		t.Fatalf("delete: %v", err)
	}

	exists, err := exchange.Exists(ctx, params)
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

	queueParams := q.MustQueueParams("test-package-list-by-queue")
	testutil.CreateQueue(t, ctx, queueParams, q.TypeFIFO, q.DeliveryPointToPoint)

	exchangeParams := x.MustExchangeParams("test-package-list-ex", x.TypeDirect)
	dx := exchange.NewDirectExchange()
	if err := dx.BindQueue(ctx, queueParams, exchangeParams, "test.key"); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	exchanges, err := exchange.ListByQueue(ctx, queueParams)
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

	ex1 := x.MustExchangeParamsWithNS("test-package-list-all-1", "ns-package-a", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-package-list-all-2", "ns-package-b", x.TypeFanout)
	if err := exchange.Create(ctx, ex1, x.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := exchange.Create(ctx, ex2, x.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	all, err := exchange.ListAll(ctx)
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

	ex1 := x.MustExchangeParamsWithNS("test-package-list-ns-1", "ns-package-list", x.TypeDirect)
	ex2 := x.MustExchangeParamsWithNS("test-package-list-ns-2", "ns-package-list", x.TypeTopic)
	if err := exchange.Create(ctx, ex1, x.PolicyStandard); err != nil {
		t.Fatalf("create ex1: %v", err)
	}
	if err := exchange.Create(ctx, ex2, x.PolicyStandard); err != nil {
		t.Fatalf("create ex2: %v", err)
	}

	exchanges, err := exchange.ListByNamespace(ctx, "ns-package-list")
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
