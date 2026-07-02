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
	"github.com/weyoss/go-redis-smq/internal/codec"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
)

// Codecs holds the codec instances for exchange serialization.
type Codecs struct {
	Params codec.SetCodec[*x.ExchangeParams]
	Props  codec.HashCodec[*x.ExchangeProps]
}

func DefaultCodecs() *Codecs {
	return &Codecs{
		Params: NewExchangeParamsCodec(),
		Props:  NewExchangePropsCodec(),
	}
}

// Manager provides Redis-backed exchange operations.
// Wires together store, lookup, and validator with configurable codecs.
type Manager struct {
	store     *Store
	lookup    *Lookup
	validator *Validator
	direct    *DirectStore
	fanout    *FanoutStore
	topic     *TopicStore
}

// NewManager creates a new exchange manager with default codecs.
func NewManager() *Manager {
	return NewManagerWithCodecs(nil)
}

// NewManagerWithCodecs creates a new exchange manager with custom codecs.
func NewManagerWithCodecs(codecs *Codecs) *Manager {
	if codecs == nil {
		codecs = DefaultCodecs()
	}

	store := NewStore(codecs)
	validator := NewValidator(store)

	return &Manager{
		store:     store,
		lookup:    NewLookup(codecs),
		validator: validator,
		direct:    NewDirectStore(store, validator, codecs),
		fanout:    NewFanoutStore(store, validator, codecs),
		topic:     NewTopicStore(store, validator, codecs),
	}
}

// Store returns the exchange store for persistence operations.
func (m *Manager) Store() *Store { return m.store }

// Lookup returns the exchange lookup for discovery operations.
func (m *Manager) Lookup() *Lookup { return m.lookup }

// Validator returns the exchange validator for binding validation.
func (m *Manager) Validator() *Validator { return m.validator }

// Direct returns the direct exchange store for routing key operations.
func (m *Manager) Direct() *DirectStore { return m.direct }

// Fanout returns the fanout exchange store for broadcast operations.
func (m *Manager) Fanout() *FanoutStore { return m.fanout }

// Topic returns the topic exchange store for pattern matching operations.
func (m *Manager) Topic() *TopicStore { return m.topic }
