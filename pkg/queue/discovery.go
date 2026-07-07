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
	"context"

	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// ListAll returns all queues across all namespaces.
func (m *Manager) ListAll(ctx context.Context) ([]q.QueueParams, error) {
	return m.lookup.All(ctx)
}

// ListByNamespace returns all queues in a namespace.
func (m *Manager) ListByNamespace(ctx context.Context, namespace string) ([]q.QueueParams, error) {
	return m.lookup.ByNamespace(ctx, namespace)
}

// Package-level convenience functions using the default manager.

// ListAll returns all queues using the default manager.
func ListAll(ctx context.Context) ([]q.QueueParams, error) {
	return defaultManager.ListAll(ctx)
}

// ListByNamespace returns namespace queues using the default manager.
func ListByNamespace(ctx context.Context, namespace string) ([]q.QueueParams, error) {
	return defaultManager.ListByNamespace(ctx, namespace)
}
