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

	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

func resolveCategory(filter q.BrowseFilter, qKey keys.Queue) (key string, counter schema.QueueField, updateMessages bool) {
	switch filter {
	case q.BrowsePending:
		return qKey.Pending(), schema.QueueFieldPendingMessagesCount, true
	case q.BrowseScheduled:
		return qKey.Scheduled(), schema.QueueFieldScheduledMessagesCount, true
	case q.BrowseAcknowledged:
		return qKey.Acknowledged(), schema.QueueFieldAcknowledgedMessagesCount, false
	case q.BrowseDeadLettered:
		return qKey.DeadLetter(), schema.QueueFieldDeadLetteredMessagesCount, false
	default:
		return "", 0, false
	}
}

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

func ptr[T any](v T) *T { return &v }
