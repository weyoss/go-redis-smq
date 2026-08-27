/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/redis"
)

// Params uniquely identifies an exchange and its routing type.
// Self-validates on creation following the same pattern as QueueParams.
// JSON serialization matches format:
//
//	{
//	  "name": "orders",
//	  "ns": "production",
//	  "type": 0
//	}
type Params struct {
	name string
	ns   string
	typ  ExchangeType
}

// NewExchangeParams creates exchange params with the default namespace.
//
// Example:
//
//	params, err := exchange.NewExchangeParams("orders", exchange.TypeDirect)
func NewExchangeParams(name string, typ ExchangeType) (*Params, error) {
	return NewExchangeParamsWithNS(name, "", typ)
}

// NewExchangeParamsWithNS creates exchange params with a custom namespace.
// Validates name and namespace using Redis key validation rules.
//
// Example:
//
//	params, err := exchange.NewExchangeParamsWithNS("orders", "production", exchange.TypeDirect)
func NewExchangeParamsWithNS(name, namespace string, typ ExchangeType) (*Params, error) {
	if name == "" {
		return nil, ErrNameRequired
	}
	if namespace == "" {
		namespace = config.Get().Namespace
	}

	validName, err := redis.ValidateKey(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidName, err.Error())
	}

	validNS, err := redis.ValidateKey(namespace)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidNamespace, err.Error())
	}

	return &Params{
		name: validName,
		ns:   validNS,
		typ:  typ,
	}, nil
}

// Name returns the exchange name.
func (p *Params) Name() string { return p.name }

// Namespace returns the exchange namespace.
func (p *Params) Namespace() string { return p.ns }

// Type returns the exchange routing type.
func (p *Params) Type() ExchangeType { return p.typ }

// Clone returns a deep copy of the exchange params.
func (p *Params) Clone() *Params {
	if p == nil {
		return nil
	}
	return &Params{
		name: p.name,
		ns:   p.ns,
		typ:  p.typ,
	}
}

// String returns the fully qualified exchange name.
// Uses the format "name@namespace".
func (p *Params) String() string {
	return p.name + "@" + p.ns
}

// MarshalJSON implements custom JSON marshaling for cross-language compatibility.
// It uses a value receiver so that both Params values and *Params pointers
// implement json.Marshaler. This ensures json.Marshal always uses the
// custom representation, even when a Params value is passed directly.
func (p Params) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name string       `json:"name"`
		NS   string       `json:"ns"`
		Type ExchangeType `json:"type"`
	}{
		Name: p.name,
		NS:   p.ns,
		Type: p.typ,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling.
// Expects: {"name":"orders","ns":"production","type":0}
func (p *Params) UnmarshalJSON(data []byte) error {
	var aux struct {
		Name string       `json:"name"`
		NS   string       `json:"ns"`
		Type ExchangeType `json:"type"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	p.name = aux.Name
	p.ns = aux.NS
	p.typ = aux.Type
	return nil
}

// MustExchangeParams creates exchange params and panics on error.
// Useful for testing and initialization where params are known to be valid.
func MustExchangeParams(name string, typ ExchangeType) *Params {
	p, err := NewExchangeParams(name, typ)
	if err != nil {
		panic(err)
	}
	return p
}

// MustExchangeParamsWithNS creates exchange params with namespace and panics on error.
func MustExchangeParamsWithNS(name, ns string, typ ExchangeType) *Params {
	p, err := NewExchangeParamsWithNS(name, ns, typ)
	if err != nil {
		panic(err)
	}
	return p
}
