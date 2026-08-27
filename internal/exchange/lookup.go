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
	"fmt"

	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Lookup handles exchange discovery and listing operations.
type Lookup struct {
	codecs *Codecs
}

// NewLookup creates a new exchange lookup with the given codecs.
// If codecs is nil, DefaultCodecs is used.
func NewLookup(codecs *Codecs) *Lookup {
	if codecs == nil {
		codecs = DefaultCodecs()
	}
	return &Lookup{codecs: codecs}
}

// All returns every exchange across all namespaces.
// Reads from the global exchanges set.
func (l *Lookup) All(ctx context.Context) ([]pubexchange.Params, error) {
	members, err := redis.LoadSetMembers(ctx,
		keys.System{}.AllExchanges(), "all exchanges")
	if err != nil {
		return nil, err
	}
	return l.decodeExchangeParams(ctx, members)
}

// ByNamespace returns all exchanges within a specific namespace.
// Reads from the namespace-scoped exchanges set.
func (l *Lookup) ByNamespace(ctx context.Context, namespace string) ([]pubexchange.Params, error) {
	members, err := redis.LoadSetMembers(ctx,
		keys.Namespace{Name: namespace}.Exchanges(),
		fmt.Sprintf("exchanges in namespace %s", namespace))
	if err != nil {
		return nil, err
	}
	return l.decodeExchangeParams(ctx, members)
}

// ByQueue returns all exchanges bound to a specific queue.
// Reads from the queue's exchange bindings set.
func (l *Lookup) ByQueue(ctx context.Context, queueParams *queue.Params) ([]pubexchange.Params, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	members, err := redis.LoadSetMembers(ctx, key.ExchangeBindings(),
		fmt.Sprintf("exchanges for queue %s", queueParams))
	if err != nil {
		return nil, err
	}
	return l.decodeExchangeParams(ctx, members)
}

// decodeExchangeParams decodes JSON-encoded exchange params from Redis set members.
// Malformed entries are silently skipped to maintain compatibility with
// potentially corrupted data from other language clients.
func (l *Lookup) decodeExchangeParams(ctx context.Context, members []string) ([]pubexchange.Params, error) {
	params := make([]pubexchange.Params, 0, len(members))
	for _, member := range members {
		p, err := l.codecs.Params.DecodeSet(ctx, member)
		if err != nil {
			// Skip malformed entries - don't fail the entire operation
			continue
		}
		// Only include entries with required fields
		if p.Name() != "" && p.Namespace() != "" {
			params = append(params, *p)
		}
	}
	return params, nil
}
