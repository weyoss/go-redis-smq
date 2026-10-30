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
	"encoding/json"
	"errors"
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/exchange"
)

// TestExchangeParams_RoundTrip_PreservesCanonicalForm mirrors the queue test.
func TestExchangeParams_RoundTrip_PreservesCanonicalForm(t *testing.T) {
	testutil.Setup(t)

	p1 := exchange.MustExchangeParamsWithNS("Orders", "Prod", exchange.TypeDirect)
	data, err := json.Marshal(p1)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var p2 exchange.Params
	if err := json.Unmarshal(data, &p2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if p1.String() != p2.String() {
		t.Fatalf("round-trip mismatch: %q vs %q", p1.String(), p2.String())
	}
	if p2.Type() != exchange.TypeDirect {
		t.Errorf("type = %v, want %v", p2.Type(), exchange.TypeDirect)
	}
}

// TestExchangeParams_Unmarshal_NormalizesUppercase is the H1 regression
// test for exchange Params.
func TestExchangeParams_Unmarshal_NormalizesUppercase(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"Orders","ns":"Prod","type":0}`)
	var p exchange.Params
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if p.Name() != "orders" {
		t.Errorf("name = %q, want %q", p.Name(), "orders")
	}
	if p.Namespace() != "prod" {
		t.Errorf("ns = %q, want %q", p.Namespace(), "prod")
	}
	if p.String() != "orders@prod" {
		t.Errorf("String() = %q, want %q", p.String(), "orders@prod")
	}
}

// TestExchangeParams_Unmarshal_EmptyNamespaceUsesConfigDefault verifies the
// config-default fallback.
func TestExchangeParams_Unmarshal_EmptyNamespaceUsesConfigDefault(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"orders","type":0}`)
	var p exchange.Params
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	expectedNS := config.Get().Namespace
	if p.Namespace() != expectedNS {
		t.Errorf("ns = %q, want config default %q", p.Namespace(), expectedNS)
	}
}

// TestExchangeParams_Unmarshal_RejectsInvalidName covers ErrInvalidName.
func TestExchangeParams_Unmarshal_RejectsInvalidName(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"3invalid","type":0}`)
	var p exchange.Params
	err := json.Unmarshal(raw, &p)
	if err == nil {
		t.Fatal("expected error for invalid name")
	}
	if !errors.Is(err, exchange.ErrInvalidName) {
		t.Errorf("err = %v, want ErrInvalidName", err)
	}
}
