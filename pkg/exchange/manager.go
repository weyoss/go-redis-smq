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

	internalExchange "github.com/weyoss/go-redis-smq/internal/exchange"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Manager provides common exchange operations.
// For type-specific operations, use DirectExchange, FanoutExchange, or TopicExchange.
type Manager struct {
	store     *internalExchange.Store
	lookup    *internalExchange.Lookup
	validator *internalExchange.Validator
}

// NewManager creates a new exchange manager with default TypeScript-compatible codecs.
func NewManager() *Manager {
	rdbManager := internalExchange.NewManager()
	return &Manager{
		store:     rdbManager.Store(),
		lookup:    rdbManager.Lookup(),
		validator: rdbManager.Validator(),
	}
}

// Create registers a new exchange with the given params and queue policy.
// The exchange type is determined by params.Type().
//
// Example:
//
//	params := x.MustExchangeParamsWithNS("orders", "production", x.TypeDirect)
//	err := exchange.Create(ctx, params, x.PolicyStandard)
func (m *Manager) Create(ctx context.Context, params *x.ExchangeParams, policy x.ExchangePolicy) error {
	return m.store.Save(ctx, params, policy)
}

// Properties retrieves the stored configuration for an exchange.
//
// Example:
//
//	props, err := exchange.Properties(ctx, params)
//	fmt.Println(props.Type, props.Policy)
func (m *Manager) Properties(ctx context.Context, params *x.ExchangeParams) (*x.ExchangeProps, error) {
	return m.store.Load(ctx, params)
}

// Exists checks whether an exchange has been created.
func (m *Manager) Exists(ctx context.Context, params *x.ExchangeParams) (bool, error) {
	return m.store.Exists(ctx, params)
}

// ValidateType verifies an exchange exists and its type matches the expected type.
// When required is false, a missing exchange does not cause an error.
//
// Example:
//
//	// Ensure exchange exists and is a direct exchange
//	err := exchange.ValidateType(ctx, params, true)
func (m *Manager) ValidateType(ctx context.Context, params *x.ExchangeParams, required bool) error {
	return m.store.ValidateType(ctx, params, required)
}

// ValidateBinding checks whether a queue can be bound to this exchange.
// Returns the exchange properties if the exchange exists, nil if it doesn't exist yet.
// Validates exchange type compatibility and queue policy constraints.
//
// Example:
//
//	props, err := exchange.ValidateBinding(ctx, exchangeParams, queueParams)
//	if props == nil {
//	    // Exchange doesn't exist yet - binding is allowed
//	}
func (m *Manager) ValidateBinding(
	ctx context.Context,
	params *x.ExchangeParams,
	queueParams *queue.QueueParams,
) (*x.ExchangeProps, error) {
	return m.validator.ValidateQueueBinding(ctx, params, queueParams)
}

// Delete removes an exchange and all its queue bindings.
// For direct/topic exchanges, also removes routing keys and pattern bindings.
//
// Example:
//
//	err := exchange.Delete(ctx, params)
func (m *Manager) Delete(ctx context.Context, params *x.ExchangeParams) error {
	return m.store.Delete(ctx, params)
}

// ListByQueue returns all exchanges bound to a specific queue.
func (m *Manager) ListByQueue(ctx context.Context, queueParams *queue.QueueParams) ([]x.ExchangeParams, error) {
	return m.lookup.ByQueue(ctx, queueParams)
}

// ListAll returns every exchange across all namespaces.
func (m *Manager) ListAll(ctx context.Context) ([]x.ExchangeParams, error) {
	return m.lookup.All(ctx)
}

// ListByNamespace returns all exchanges within a namespace.
func (m *Manager) ListByNamespace(ctx context.Context, namespace string) ([]x.ExchangeParams, error) {
	return m.lookup.ByNamespace(ctx, namespace)
}

// defaultManager is the shared instance used by package-level convenience functions.
var defaultManager = NewManager()

// Create registers a new exchange using the default manager.
func Create(ctx context.Context, params *x.ExchangeParams, policy x.ExchangePolicy) error {
	return defaultManager.Create(ctx, params, policy)
}

// Properties retrieves exchange properties using the default manager.
func Properties(ctx context.Context, params *x.ExchangeParams) (*x.ExchangeProps, error) {
	return defaultManager.Properties(ctx, params)
}

// Exists checks exchange existence using the default manager.
func Exists(ctx context.Context, params *x.ExchangeParams) (bool, error) {
	return defaultManager.Exists(ctx, params)
}

// ValidateType verifies exchange type using the default manager.
func ValidateType(ctx context.Context, params *x.ExchangeParams, required bool) error {
	return defaultManager.ValidateType(ctx, params, required)
}

// ValidateBinding checks queue binding using the default manager.
func ValidateBinding(ctx context.Context, params *x.ExchangeParams, queueParams *queue.QueueParams) (*x.ExchangeProps, error) {
	return defaultManager.ValidateBinding(ctx, params, queueParams)
}

// Delete removes an exchange using the default manager.
func Delete(ctx context.Context, params *x.ExchangeParams) error {
	return defaultManager.Delete(ctx, params)
}

// ListByQueue returns queue exchanges using the default manager.
func ListByQueue(ctx context.Context, queueParams *queue.QueueParams) ([]x.ExchangeParams, error) {
	return defaultManager.ListByQueue(ctx, queueParams)
}

// ListAll returns all exchanges using the default manager.
func ListAll(ctx context.Context) ([]x.ExchangeParams, error) {
	return defaultManager.ListAll(ctx)
}

// ListByNamespace returns namespace exchanges using the default manager.
func ListByNamespace(ctx context.Context, namespace string) ([]x.ExchangeParams, error) {
	return defaultManager.ListByNamespace(ctx, namespace)
}
