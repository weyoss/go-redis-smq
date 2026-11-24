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
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
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
	// defaultRecoveryInterval is how often the purge worker scans
	// ActivePurgeJobs for jobs whose worker is no longer alive.
	//
	// Tied to workerHeartbeatTTL: a live worker's heartbeat is refreshed every
	// workerHeartbeatInterval (10s) and expires workerHeartbeatTTL (30s) after
	// death. Scanning at the TTL cadence gives a worst-case recovery lag of
	// 2 × TTL (60s) while costing nothing on the common case (LRange on an
	// empty list).
	defaultRecoveryInterval = workerHeartbeatTTL
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

// resolveCategoryKey maps a browse filter to its Redis key.
//
// For BrowsePending, the key depends on two payload fields:
//
//   - queueType: priority queues store pending messages in a sorted set
//     (Priority / PriorityWithGroup); FIFO and LIFO store them in a
//     list (Pending / PendingWithGroup). Reading the wrong structure
//     returns nothing — a priority purge that reads the FIFO list, or
//     vice versa, is silently a no-op.
//
//   - groupID: Pub/Sub messages live in per-group lists. groupID == ""
//     is correct only for point-to-point queues.
//
// The other categories (scheduled, acknowledged, dead-lettered) are
// queue-level and ignore both fields — a scheduled or dead-lettered
// message is not scoped to a consumer group.
func resolveCategoryKey(
	filter publicqueue.BrowseFilter,
	qKey keys.Queue,
	queueType publicqueue.Type,
	groupID string,
) string {
	switch filter {
	case publicqueue.BrowsePending:
		if queueType == publicqueue.TypePriority {
			if groupID != "" {
				return qKey.PriorityWithGroup(groupID)
			}
			return qKey.Priority()
		}
		if groupID != "" {
			return qKey.PendingWithGroup(groupID)
		}
		return qKey.Pending()

	case publicqueue.BrowseScheduled:
		return qKey.Scheduled()

	case publicqueue.BrowseAcknowledged:
		return qKey.Acknowledged()

	case publicqueue.BrowseDeadLettered:
		return qKey.DeadLetter()

	default:
		return ""
	}
}

// forceRemoveFromCategory removes the given IDs from a category key.
//
// The key may be a list (pending, acknowledged, dead-lettered, requeued)
// or a sorted set (scheduled, priority pending). For lists: pipelined
// LREM per ID (one round-trip). For sorted sets: bulk ZREM.
// LREM/ZREM on non-existent elements are no-ops, so calling this with
// IDs that were already removed (by delete-message.lua for successful
// deletes, or by a concurrent peer) is safe.
//
// Called by purgeMessages only when DeleteMessages reports NotFound > 0.
// Zero cost in the normal case (no orphans).
func forceRemoveFromCategory(ctx context.Context, key string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	t, err := redis.Client().Type(ctx, key).Result()
	if err != nil {
		return err
	}
	switch t {
	case "list":
		pipe := redis.Client().Pipeline()
		for _, id := range ids {
			pipe.LRem(ctx, key, 1, id)
		}
		_, err := pipe.Exec(ctx)
		return err
	case "zset":
		members := make([]interface{}, len(ids))
		for i, id := range ids {
			members[i] = id
		}
		return redis.Client().ZRem(ctx, key, members...).Err()
	case "none":
		return nil
	default:
		return fmt.Errorf("force-remove: unexpected key type: %s", t)
	}
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T { return &v }
