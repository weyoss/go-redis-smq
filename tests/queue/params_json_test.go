/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/redis"
)

// TestParams_RoundTrip_PreservesCanonicalForm verifies that the
// constructor's canonical form survives a Marshal→Unmarshal round-trip
// unchanged.
func TestParams_RoundTrip_PreservesCanonicalForm(t *testing.T) {
	testutil.Setup(t)

	p1 := publicqueue.MustQueueParamsWithNS("Orders", "Prod")
	data, err := json.Marshal(p1)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var p2 publicqueue.Params
	if err := json.Unmarshal(data, &p2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if p1.String() != p2.String() {
		t.Fatalf("round-trip mismatch: %q vs %q", p1.String(), p2.String())
	}
	if p2.Name() != "orders" {
		t.Errorf("name = %q, want %q", p2.Name(), "orders")
	}
	if p2.NS() != "prod" {
		t.Errorf("ns = %q, want %q", p2.NS(), "prod")
	}
}

// TestParams_Unmarshal_NormalizesUppercase verifies that a JSON payload
// with uppercase name/namespace (e.g. written by a non-lowercasing
// producer, or a legacy value in Redis) decodes to the same canonical
// form the constructor would produce.
//
// Regression test for H1: pre-fix, UnmarshalJSON copied raw strings, so
// this would produce Name()=="Orders" and NS()=="Prod" — diverging from
// the constructor's canonical form.
func TestParams_Unmarshal_NormalizesUppercase(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"Orders","ns":"Prod"}`)
	var p publicqueue.Params
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if p.Name() != "orders" {
		t.Errorf("name = %q, want %q (uppercase not normalized)", p.Name(), "orders")
	}
	if p.NS() != "prod" {
		t.Errorf("ns = %q, want %q (uppercase not normalized)", p.NS(), "prod")
	}
	if p.String() != "orders@prod" {
		t.Errorf("String() = %q, want %q", p.String(), "orders@prod")
	}

	// Cross-check: the decoded Params must produce the same String() as
	// the constructor with the same input.
	constructed := publicqueue.MustQueueParamsWithNS("Orders", "Prod")
	if p.String() != constructed.String() {
		t.Fatalf("decoded %q != constructed %q", p.String(), constructed.String())
	}
}

// TestParams_Unmarshal_EmptyNamespaceUsesConfigDefault verifies that a
// JSON payload without an "ns" field falls back to the config default,
// matching the constructor's behavior.
func TestParams_Unmarshal_EmptyNamespaceUsesConfigDefault(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"orders"}`)
	var p publicqueue.Params
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	expectedNS := config.Get().Namespace
	if p.NS() != expectedNS {
		t.Errorf("ns = %q, want config default %q", p.NS(), expectedNS)
	}
}

// TestParams_Unmarshal_RejectsEmptyName verifies that a JSON payload
// without a name fails, matching the constructor's ErrNameRequired.
func TestParams_Unmarshal_RejectsEmptyName(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"","ns":"prod"}`)
	var p publicqueue.Params
	err := json.Unmarshal(raw, &p)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	if !errors.Is(err, publicqueue.ErrNameRequired) {
		t.Errorf("err = %v, want ErrNameRequired", err)
	}
}

// TestParams_Unmarshal_RejectsInvalidName verifies that names failing
// redis.ValidateKey are rejected with ErrInvalidName, matching the
// constructor.
func TestParams_Unmarshal_RejectsInvalidName(t *testing.T) {
	testutil.Setup(t)

	cases := []string{
		`{"name":"3invalid","ns":"prod"}`, // starts with digit
		`{"name":"-leading-hyphen","ns":"prod"}`,
		`{"name":"invalid space","ns":"prod"}`,
	}
	for _, raw := range cases {
		var p publicqueue.Params
		err := json.Unmarshal([]byte(raw), &p)
		if err == nil {
			t.Errorf("%s: expected error, got nil (p.String()=%q)", raw, p.String())
			continue
		}
		if !errors.Is(err, publicqueue.ErrInvalidName) {
			t.Errorf("%s: err = %v, want ErrInvalidName", raw, err)
		}
		// The inner redis.ErrInvalidKey should also be reachable via
		// errors.Is, because NewQueueParamsWithNS wraps with %w: %w.
		if !errors.Is(err, redis.ErrInvalidKey) {
			t.Errorf("%s: err = %v, want to also match redis.ErrInvalidKey", raw, err)
		}
	}
}

// TestParams_Unmarshal_RejectsInvalidNamespace verifies that namespaces
// failing redis.ValidateKey are rejected with ErrInvalidNamespace.
func TestParams_Unmarshal_RejectsInvalidNamespace(t *testing.T) {
	testutil.Setup(t)

	raw := []byte(`{"name":"orders","ns":"3invalid"}`)
	var p publicqueue.Params
	err := json.Unmarshal(raw, &p)
	if err == nil {
		t.Fatal("expected error for invalid namespace")
	}
	if !errors.Is(err, publicqueue.ErrInvalidNamespace) {
		t.Errorf("err = %v, want ErrInvalidNamespace", err)
	}
}
