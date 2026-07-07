/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package q

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/pkg/config"
)

// QueueParams uniquely identifies a queue within a namespace.
// Self-validates on creation using Redis key validation rules.
//
//	{
//	  "name": "orders",
//	  "ns": "production"
//	}
type QueueParams struct {
	name string
	ns   string
}

// NewQueueParams creates queue params with the default namespace.
//
// Example:
//
//	params, err := queue.NewQueueParams("orders")
func NewQueueParams(name string) (*QueueParams, error) {
	return NewQueueParamsWithNS(name, "")
}

// NewQueueParamsWithNS creates queue params with a custom namespace.
// Validates name and namespace using Redis key validation rules.
//
// Example:
//
//	params, err := queue.NewQueueParamsWithNS("orders", "production")
func NewQueueParamsWithNS(name, namespace string) (*QueueParams, error) {
	if name == "" {
		return nil, ErrNameRequired
	}
	if namespace == "" {
		namespace = config.Get().Namespace
	}

	validName, err := keys.ValidateKey(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidName, err.Error())
	}

	validNS, err := keys.ValidateKey(namespace)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidNamespace, err.Error())
	}

	return &QueueParams{
		name: validName,
		ns:   validNS,
	}, nil
}

// Name returns the queue name.
func (p *QueueParams) Name() string { return p.name }

// NS returns the queue namespace.
func (p *QueueParams) NS() string { return p.ns }

// Clone returns a deep copy of the queue params.
func (p *QueueParams) Clone() *QueueParams {
	if p == nil {
		return nil
	}
	return &QueueParams{
		name: p.name,
		ns:   p.ns,
	}
}

// String returns the fully qualified queue name.
// Uses the format "name@namespace" for non-default namespaces.
func (p *QueueParams) String() string {
	return p.name + "@" + p.ns
}

// MarshalJSON implements custom JSON marshaling
// Produces: {"name":"orders","ns":"production"}
func (p QueueParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name string `json:"name"`
		NS   string `json:"ns"`
	}{
		Name: p.name,
		NS:   p.ns,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
// Expects: {"name":"orders","ns":"production"}
func (p *QueueParams) UnmarshalJSON(data []byte) error {
	var aux struct {
		Name string `json:"name"`
		NS   string `json:"ns"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	p.name = aux.Name
	p.ns = aux.NS
	return nil
}

// MustQueueParams creates queue params and panics on error.
// Useful for testing and initialization where params are known to be valid.
func MustQueueParams(name string) *QueueParams {
	p, err := NewQueueParams(name)
	if err != nil {
		panic(err)
	}
	return p
}

// MustQueueParamsWithNS creates queue params with namespace and panics on error.
func MustQueueParamsWithNS(name, ns string) *QueueParams {
	p, err := NewQueueParamsWithNS(name, ns)
	if err != nil {
		panic(err)
	}
	return p
}
