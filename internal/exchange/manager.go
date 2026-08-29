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
	"context"

	"github.com/weyoss/go-redis-smq/internal/codec"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Codecs holds the codec instances for exchange serialization.
type Codecs struct {
	Params codec.SetCodec[*pubexchange.Params]
	Props  codec.HashCodec[*pubexchange.Props]
}

func DefaultCodecs() *Codecs {
	return &Codecs{
		Params: NewExchangeParamsCodec(),
		Props:  NewExchangePropsCodec(),
	}
}

// Manager provides Redis-backed exchange operations.
// It implements the pubexchange.Manager interface and wires together
// store, lookup, validator, and type-specific sub-stores.
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

// Create registers a new exchange with the given params and queue policy.
func (m *Manager) Create(ctx context.Context, params *pubexchange.Params, policy pubexchange.Policy) error {
	return m.store.Save(ctx, params, policy)
}

// Properties retrieves the stored configuration for an exchange.
func (m *Manager) Properties(ctx context.Context, params *pubexchange.Params) (*pubexchange.Props, error) {
	return m.store.Load(ctx, params)
}

// Exists checks whether an exchange has been created.
func (m *Manager) Exists(ctx context.Context, params *pubexchange.Params) (bool, error) {
	return m.store.Exists(ctx, params)
}

// ValidateType verifies an exchange exists and its type matches the expected type.
func (m *Manager) ValidateType(ctx context.Context, params *pubexchange.Params, required bool) error {
	return m.store.ValidateType(ctx, params, required)
}

// ValidateBinding checks whether a queue can be bound to this exchange.
func (m *Manager) ValidateBinding(
	ctx context.Context,
	params *pubexchange.Params,
	queueParams *queue.Params,
) (*pubexchange.Props, error) {
	return m.validator.ValidateQueueBinding(ctx, params, queueParams)
}

// Delete removes an exchange and all its queue bindings.
func (m *Manager) Delete(ctx context.Context, params *pubexchange.Params) error {
	return m.store.Delete(ctx, params)
}

// ListByQueue returns all exchanges bound to a specific queue.
func (m *Manager) ListByQueue(ctx context.Context, queueParams *queue.Params) ([]pubexchange.Params, error) {
	return m.lookup.ByQueue(ctx, queueParams)
}

// ListAll returns every exchange across all namespaces.
func (m *Manager) ListAll(ctx context.Context) ([]pubexchange.Params, error) {
	return m.lookup.All(ctx)
}

// ListByNamespace returns all exchanges within a namespace.
func (m *Manager) ListByNamespace(ctx context.Context, namespace string) ([]pubexchange.Params, error) {
	return m.lookup.ByNamespace(ctx, namespace)
}
