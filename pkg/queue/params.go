/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"encoding/json"
	"fmt"

	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/redis"
)

// Params uniquely identifies a queue within a namespace.
//
// The type self-validates on creation. Validation rules are the same as
// Redis key rules: lowercase letters, digits, hyphens, underscores, and
// dots. The first character must be a letter.
type Params struct {
	name string
	ns   string
}

// NewQueueParams creates queue params with the default namespace.
// It validates the queue name and returns an error if invalid.
func NewQueueParams(name string) (*Params, error) {
	return NewQueueParamsWithNS(name, "")
}

// NewQueueParamsWithNS creates queue params with a custom namespace.
// If namespace is empty, the configured default namespace is used.
// It validates both name and namespace and returns an error if invalid.
func NewQueueParamsWithNS(name, namespace string) (*Params, error) {
	if name == "" {
		return nil, ErrNameRequired
	}
	if namespace == "" {
		namespace = config.Get().Namespace
	}

	validName, err := redis.ValidateKey(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidName, err)
	}

	validNS, err := redis.ValidateKey(namespace)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidNamespace, err)
	}

	return &Params{
		name: validName,
		ns:   validNS,
	}, nil
}

// Name returns the queue name.
func (p *Params) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}

// NS returns the queue namespace.
func (p *Params) NS() string {
	if p == nil {
		return ""
	}
	return p.ns
}

// Clone returns a deep copy of the queue params.
func (p *Params) Clone() *Params {
	if p == nil {
		return nil
	}
	return &Params{name: p.name, ns: p.ns}
}

// String returns the fully qualified queue name (name@namespace).
func (p *Params) String() string {
	if p == nil {
		return ""
	}
	return p.name + "@" + p.ns
}

// MarshalJSON implements custom JSON marshaling for TypeScript compatibility.
func (p Params) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name string `json:"name"`
		NS   string `json:"ns"`
	}{
		Name: p.name,
		NS:   p.ns,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for TypeScript compatibility.
func (p *Params) UnmarshalJSON(data []byte) error {
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
func MustQueueParams(name string) *Params {
	p, err := NewQueueParams(name)
	if err != nil {
		panic(err)
	}
	return p
}

// MustQueueParamsWithNS creates queue params with namespace and panics on error.
func MustQueueParamsWithNS(name, ns string) *Params {
	p, err := NewQueueParamsWithNS(name, ns)
	if err != nil {
		panic(err)
	}
	return p
}
