/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package namespace

import (
	"context"
	"errors"
	"fmt"

	internalExchange "github.com/weyoss/go-redis-smq/internal/exchange"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	pubnamespace "github.com/weyoss/go-redis-smq/pkg/namespace"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Manager is the concrete implementation of the public namespace.Manager.
type Manager struct {
	queueLookup    *internalQueue.Lookup
	exchangeLookup *internalExchange.Lookup
	queueStore     *internalQueue.Store
	exchangeStore  *internalExchange.Store
}

// NewManager creates a new namespace manager.
func NewManager() *Manager {
	queueMgr := internalQueue.NewManager()
	exchangeMgr := internalExchange.NewManager()

	return &Manager{
		queueLookup:    queueMgr.Lookup(),
		exchangeLookup: exchangeMgr.Lookup(),
		queueStore:     queueMgr.Store(),
		exchangeStore:  exchangeMgr.Store(),
	}
}

// List returns all registered namespaces.
func (m *Manager) List(ctx context.Context) ([]string, error) {
	return m.queueLookup.AllNamespaces(ctx)
}

// Exists checks whether a namespace exists.
func (m *Manager) Exists(ctx context.Context, name string) (bool, error) {
	validName, err := validate(name)
	if err != nil {
		return false, err
	}

	exists, err := redisClient.SetContains(ctx, keys.System{}.AllNamespaces(), validName, "namespace")
	if err != nil {
		return false, fmt.Errorf("check namespace exists: %w", err)
	}
	return exists, nil
}

// Delete removes a namespace and all its queues and exchanges.
func (m *Manager) Delete(ctx context.Context, name string) error {
	validName, err := validate(name)
	if err != nil {
		return err
	}

	exists, err := m.Exists(ctx, validName)
	if err != nil {
		return err
	}
	if !exists {
		return pubnamespace.ErrNotFound
	}

	// Delete all queues in the namespace
	queues, err := m.queueLookup.ByNamespace(ctx, validName)
	if err != nil {
		return fmt.Errorf("get namespace queues: %w", err)
	}

	for _, qp := range queues {
		if err := m.queueStore.Delete(ctx, &qp); err != nil {
			if errors.Is(err, pubnamespace.ErrNotFound) {
				continue
			}
			return fmt.Errorf("delete queue %s: %w", qp.Name(), err)
		}
	}

	// Delete all exchanges in the namespace
	exchanges, err := m.exchangeLookup.ByNamespace(ctx, validName)
	if err != nil {
		return fmt.Errorf("get namespace exchanges: %w", err)
	}

	for _, ep := range exchanges {
		if err := m.exchangeStore.Delete(ctx, &ep); err != nil {
			if errors.Is(err, pubnamespace.ErrNotFound) {
				continue
			}
			return fmt.Errorf("delete exchange %s: %w", ep.Name(), err)
		}
	}

	// Remove the namespace from the global registry
	if err := redisClient.Client().SRem(ctx, keys.System{}.AllNamespaces(), validName).Err(); err != nil {
		return fmt.Errorf("remove namespace: %w", err)
	}

	return nil
}

// ListQueues returns all queues in a namespace.
func (m *Manager) ListQueues(ctx context.Context, name string) ([]queue.QueueParams, error) {
	validName, err := validate(name)
	if err != nil {
		return nil, err
	}
	return m.queueLookup.ByNamespace(ctx, validName)
}

// ListExchanges returns all exchanges in a namespace.
func (m *Manager) ListExchanges(ctx context.Context, name string) ([]pubexchange.ExchangeParams, error) {
	validName, err := validate(name)
	if err != nil {
		return nil, err
	}
	return m.exchangeLookup.ByNamespace(ctx, validName)
}

// validate validates a namespace name.
func validate(name string) (string, error) {
	if name == "" {
		return "", pubnamespace.ErrNameRequired
	}

	validName, err := keys.ValidateKey(name)
	if err != nil {
		return "", fmt.Errorf("%w: %s", pubnamespace.ErrInvalidName, err.Error())
	}

	return validName, nil
}
