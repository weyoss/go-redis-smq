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

	"github.com/weyoss/go-redis-smq/internal/config"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

const defaultBrowseCount = 100

// Browse provides message browsing operations for queues.
type Browse struct {
	store *Store
}

func NewBrowse(store *Store) *Browse {
	return &Browse{store: store}
}

func (b *Browse) BrowseMessages(
	ctx context.Context,
	queueParams *publicqueue.Params,
	params *publicqueue.BrowseParams,
) (*publicqueue.BrowseResult, error) {
	if params == nil {
		params = &publicqueue.BrowseParams{Filter: publicqueue.BrowsePublished}
	}
	if params.Count <= 0 {
		params.Count = defaultBrowseCount
	}

	props, err := b.store.Load(ctx, queueParams)
	if err != nil {
		return nil, fmt.Errorf("browse: load queue: %w", err)
	}

	qKey := keys.Queue{Namespace: queueParams.NS(), Name: queueParams.Name()}

	switch params.Filter {
	case publicqueue.BrowsePublished:
		return b.browseList(ctx, qKey.Published(), params)
	case publicqueue.BrowsePending:
		switch props.Type {
		case publicqueue.TypePriority:
			return b.browseSortedSet(ctx, qKey.Priority(), params)
		default:
			return b.browseList(ctx, qKey.Pending(), params)
		}
	case publicqueue.BrowseScheduled:
		return b.browseSortedSet(ctx, qKey.Scheduled(), params)
	case publicqueue.BrowseAcknowledged:
		if !config.Get().MessageAudit.AcknowledgedMessages.Enabled {
			return nil, fmt.Errorf("browse: acknowledged messages audit is disabled")
		}
		return b.browseList(ctx, qKey.Acknowledged(), params)
	case publicqueue.BrowseDeadLettered:
		if !config.Get().MessageAudit.DeadLetteredMessages.Enabled {
			return nil, fmt.Errorf("browse: dead-lettered messages audit is disabled")
		}
		return b.browseList(ctx, qKey.DeadLetter(), params)
	default:
		return nil, fmt.Errorf("browse: unknown filter: %v", params.Filter)
	}
}

func (b *Browse) browseList(
	ctx context.Context,
	key string,
	params *publicqueue.BrowseParams,
) (*publicqueue.BrowseResult, error) {
	total, err := redisClient.Client().LLen(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("browse list: %w", err)
	}

	stop := params.Offset + params.Count - 1
	ids, err := redisClient.Client().LRange(ctx, key, params.Offset, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("browse list: %w", err)
	}

	return &publicqueue.BrowseResult{
		IDs:     ids,
		Total:   total,
		Offset:  params.Offset,
		Count:   int64(len(ids)),
		HasMore: params.Offset+int64(len(ids)) < total,
	}, nil
}

func (b *Browse) browseSortedSet(
	ctx context.Context,
	key string,
	params *publicqueue.BrowseParams,
) (*publicqueue.BrowseResult, error) {
	total, err := redisClient.Client().ZCard(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("browse sorted set: %w", err)
	}

	stop := params.Offset + params.Count - 1
	ids, err := redisClient.Client().ZRange(ctx, key, params.Offset, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("browse sorted set: %w", err)
	}

	return &publicqueue.BrowseResult{
		IDs:     ids,
		Total:   total,
		Offset:  params.Offset,
		Count:   int64(len(ids)),
		HasMore: params.Offset+int64(len(ids)) < total,
	}, nil
}
