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
	"encoding/json"
	"fmt"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	x "github.com/weyoss/go-redis-smq/pkg/exchange"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Lookup handles queue discovery and listing operations.
type Lookup struct {
	codecs *Codecs
}

// NewLookup creates a new queue lookup with the given codecs.
// If codecs is nil, DefaultCodecs is used.
func NewLookup(codecs *Codecs) *Lookup {
	if codecs == nil {
		codecs = DefaultCodecs()
	}
	return &Lookup{codecs: codecs}
}

// All returns every queue across all namespaces.
func (l *Lookup) All(ctx context.Context) ([]publicqueue.Params, error) {
	members, err := redisClient.LoadSetMembers(ctx,
		keys.System{}.AllQueues(), "all queues")
	if err != nil {
		return nil, err
	}
	return DecodeQueueParams(members)
}

// ByNamespace returns all queues within a specific namespace.
func (l *Lookup) ByNamespace(ctx context.Context, namespace string) ([]publicqueue.Params, error) {
	members, err := redisClient.LoadSetMembers(ctx,
		keys.Namespace{Name: namespace}.Queues(),
		fmt.Sprintf("queues in namespace %s", namespace))
	if err != nil {
		return nil, err
	}
	return DecodeQueueParams(members)
}

// ByExchange returns all queues bound to a specific exchange.
func (l *Lookup) ByExchange(ctx context.Context, namespace, exchangeName string, exchangeType int) ([]publicqueue.Params, error) {
	exKey := keys.Exchange{
		Namespace: namespace,
		Name:      exchangeName,
	}

	var allMembers []string

	switch exchangeType {
	case x.TypeDirect.Int():
		routingKeys, err := redisClient.LoadSetMembers(ctx, exKey.RoutingKeys(),
			fmt.Sprintf("routing keys for exchange %s/%s", namespace, exchangeName))
		if err != nil {
			return nil, err
		}

		for _, rk := range routingKeys {
			members, err := redisClient.LoadSetMembers(ctx,
				exKey.RoutingKeyQueues(rk),
				fmt.Sprintf("queues for routing key %s", rk))
			if err != nil {
				return nil, err
			}
			allMembers = append(allMembers, members...)
		}

	case x.TypeTopic.Int():
		patterns, err := redisClient.LoadSetMembers(ctx, exKey.BindingPatterns(),
			fmt.Sprintf("patterns for exchange %s/%s", namespace, exchangeName))
		if err != nil {
			return nil, err
		}

		for _, pattern := range patterns {
			members, err := redisClient.LoadSetMembers(ctx,
				exKey.PatternQueues(pattern),
				fmt.Sprintf("queues for pattern %s", pattern))
			if err != nil {
				return nil, err
			}
			allMembers = append(allMembers, members...)
		}

	case x.TypeFanout.Int():
		members, err := redisClient.LoadSetMembers(ctx, exKey.FanoutQueues(),
			fmt.Sprintf("queues for fanout exchange %s/%s", namespace, exchangeName))
		if err != nil {
			return nil, err
		}
		allMembers = members
	}

	// Deduplicate queue params
	seen := make(map[string]bool)
	var uniqueMembers []string
	for _, member := range allMembers {
		if !seen[member] {
			seen[member] = true
			uniqueMembers = append(uniqueMembers, member)
		}
	}

	return DecodeQueueParams(uniqueMembers)
}

// AllNamespaces returns all registered namespaces.
func (l *Lookup) AllNamespaces(ctx context.Context) ([]string, error) {
	members, err := redisClient.LoadSetMembers(ctx,
		keys.System{}.AllNamespaces(), "all namespaces")
	if err != nil {
		return nil, err
	}
	return members, nil
}

// DecodeQueueParams decodes JSON-encoded queue params from Redis set members.
// Malformed entries are silently skipped.
func DecodeQueueParams(members []string) ([]publicqueue.Params, error) {
	params := make([]publicqueue.Params, 0, len(members))
	for _, member := range members {
		var p publicqueue.Params
		if err := json.Unmarshal([]byte(member), &p); err != nil {
			continue
		}
		if p.Name() != "" && p.NS() != "" {
			params = append(params, p)
		}
	}
	return params, nil
}
