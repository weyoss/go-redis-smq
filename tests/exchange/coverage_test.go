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

	"github.com/weyoss/go-redis-smq/pkg/exchange"
)

func TestExchange_TypeStringAndIsValid(t *testing.T) {
	if exchange.TypeDirect.String() != "direct" {
		t.Errorf("TypeDirect.String() = %q", exchange.TypeDirect.String())
	}
	if exchange.TypeFanout.String() != "fanout" {
		t.Errorf("TypeFanout.String() = %q", exchange.TypeFanout.String())
	}
	if exchange.TypeTopic.String() != "topic" {
		t.Errorf("TypeTopic.String() = %q", exchange.TypeTopic.String())
	}
	if exchange.ExchangeType(99).String() != "unknown" {
		t.Errorf("invalid type string = %q", exchange.ExchangeType(99).String())
	}

	for _, typ := range []exchange.ExchangeType{exchange.TypeDirect, exchange.TypeFanout, exchange.TypeTopic} {
		if !typ.IsValid() {
			t.Errorf("%v should be valid", typ)
		}
	}
	if exchange.ExchangeType(99).IsValid() {
		t.Error("invalid type should not be valid")
	}
}

func TestExchange_PolicyStringAndIsValid(t *testing.T) {
	if exchange.PolicyStandard.String() != "standard" {
		t.Errorf("PolicyStandard.String() = %q", exchange.PolicyStandard.String())
	}
	if exchange.PolicyPriority.String() != "priority" {
		t.Errorf("PolicyPriority.String() = %q", exchange.PolicyPriority.String())
	}
	if exchange.ExchangePolicy(99).String() != "unknown" {
		t.Errorf("invalid policy string = %q", exchange.ExchangePolicy(99).String())
	}
	if !exchange.PolicyStandard.IsValid() || !exchange.PolicyPriority.IsValid() {
		t.Error("valid policies should be valid")
	}
	if exchange.ExchangePolicy(99).IsValid() {
		t.Error("invalid policy should not be valid")
	}
}

func TestExchange_ParamsClone(t *testing.T) {
	params := exchange.MustExchangeParamsWithNS("orders", "production", exchange.TypeTopic)
	clone := params.Clone()
	if clone == nil {
		t.Fatal("Clone returned nil")
	}
	if clone == params {
		t.Error("Clone should return a new instance")
	}
	if clone.Name() != params.Name() || clone.Namespace() != params.Namespace() || clone.Type() != params.Type() {
		t.Errorf("Clone fields mismatch")
	}
}
