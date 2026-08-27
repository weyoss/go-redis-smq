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

	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Manager provides namespace-level operations.
type Manager interface {
	// List returns all registered namespaces.
	List(ctx context.Context) ([]string, error)

	// Exists checks whether a namespace exists (has at least one queue or exchange).
	Exists(ctx context.Context, name string) (bool, error)

	// Delete removes a namespace and all its queues and exchanges.
	Delete(ctx context.Context, name string) error

	// ListQueues returns all queues in a namespace.
	ListQueues(ctx context.Context, name string) ([]queue.Params, error)

	// ListExchanges returns all exchanges in a namespace.
	ListExchanges(ctx context.Context, name string) ([]exchange.Params, error)
}
