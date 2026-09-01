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
	"fmt"
	"time"

	"github.com/weyoss/go-redis-smq/internal/redis"
)

const (
	// defaultPurgeBatchSize is the default number of messages deleted per batch.
	defaultPurgeBatchSize = 1000
	// defaultPurgeBatchDelay is the default delay between batches.
	defaultPurgeBatchDelay = 5 * time.Second
	// workerHeartbeatInterval is how often the purge worker sends a heartbeat.
	workerHeartbeatInterval = 10 * time.Second
	// workerHeartbeatTTL is the expiry time for the purge worker heartbeat key.
	workerHeartbeatTTL = 30 * time.Second
	// popTimeout is the blocking pop timeout used by the purge worker.
	popTimeout = 1 * time.Second
)

// fetchBatch retrieves up to count message IDs from the given Redis key.
//
// The key may be a Redis list or sorted set. If the key does not exist,
// an empty slice and nil error are returned.
func fetchBatch(ctx context.Context, key string, count int) ([]string, error) {
	t, err := redis.Client().Type(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	switch t {
	case "list":
		return redis.Client().LRange(ctx, key, 0, int64(count-1)).Result()
	case "zset":
		return redis.Client().ZRange(ctx, key, 0, int64(count-1)).Result()
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("purge: unexpected key type: %s", t)
	}
}

// trimCategory removes the first count entries from a Redis list or sorted set.
//
// It is used after a batch of messages has been processed to remove them
// from the category list. For lists, LTRIM is used; for sorted sets, ZREM
// removes the specified members.
func trimCategory(ctx context.Context, key string, count int64) error {
	t, err := redis.Client().Type(ctx, key).Result()
	if err != nil {
		return err
	}
	switch t {
	case "list":
		return redis.Client().LTrim(ctx, key, count, -1).Err()
	case "zset":
		ids, err := redis.Client().ZRange(ctx, key, 0, count-1).Result()
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		members := make([]interface{}, len(ids))
		for i, id := range ids {
			members[i] = id
		}
		return redis.Client().ZRem(ctx, key, members...).Err()
	}
	return nil
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T { return &v }
