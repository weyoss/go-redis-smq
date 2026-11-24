/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package ratelimit

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Store handles rate limit persistence for queues.
type Store struct{}

// NewStore creates a new rate limit store.
func NewStore() *Store {
	return &Store{}
}

// Set stores the given rate limit on a queue.
func (s *Store) Set(ctx context.Context, queueParams *publicqueue.Params, rl *publicqueue.RateLimitParams) error {
	queueKeys := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	rateLimitJSON, err := MarshalRateLimitParams(ctx, rl)
	if err != nil {
		return fmt.Errorf("set rate limit: marshal: %w", err)
	}

	luaKeys := []string{queueKeys.Properties()}

	argv := []interface{}{
		qSchema.RateLimit.Key(),
		rateLimitJSON,
		qSchema.OperationalState.Key(),
		publicqueue.StateLocked.String(),
		qSchema.LockID.Key(),
		"",
	}

	reply, err := redisClient.Eval(ctx, scripts.SetRateLimit, luaKeys, argv...)
	if err != nil {
		return fmt.Errorf("set rate limit: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		return err
	}

	switch replyStr {
	case "OK":
		return nil
	case "QUEUE_LOCKED":
		return publicqueue.ErrLocked
	case "QUEUE_NOT_FOUND":
		return publicqueue.ErrNotFound
	default:
		return fmt.Errorf("set rate limit: unexpected script reply: %s", replyStr)
	}
}

// Clear removes the rate limit from a queue.
func (s *Store) Clear(ctx context.Context, queueParams *publicqueue.Params) error {
	queueKeys := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	luaKeys := []string{
		queueKeys.Properties(),
		queueKeys.RateLimit(),
	}

	argv := []interface{}{
		qSchema.RateLimit.Key(),
		qSchema.OperationalState.Key(),
		publicqueue.StateLocked.String(),
		qSchema.LockID.Key(),
		"",
	}

	reply, err := redisClient.Eval(ctx, scripts.ClearRateLimit, luaKeys, argv...)
	if err != nil {
		return fmt.Errorf("clear rate limit: %w", err)
	}

	replyStr, err := redisClient.String(reply)
	if err != nil {
		return err
	}

	switch replyStr {
	case "OK":
		return nil
	case "QUEUE_LOCKED":
		return publicqueue.ErrLocked
	case "QUEUE_NOT_FOUND":
		return publicqueue.ErrNotFound
	default:
		return fmt.Errorf("clear rate limit: unexpected script reply: %s", replyStr)
	}
}

// Get retrieves the current rate limit for a queue.
//
// Returns (nil, nil) only when the queue exists and has no rate limit
// configured.
func (s *Store) Get(ctx context.Context, queueParams *publicqueue.Params) (*publicqueue.RateLimitParams, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}.Properties()

	rateLimitJSON, err := redisClient.LoadHashField(ctx, key,
		qSchema.RateLimit.Key(), "rate limit")
	if err != nil {
		// Not redis.Nil: a real transport / connection error. Propagate.
		// Pre-fix swallowed this as (nil, nil), masking Redis outages as
		// "no rate limit".
		if !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("get rate limit: %w", err)
		}

		// redis.Nil: field absent OR key absent. Distinguish.
		exists, existsErr := redisClient.Client().Exists(ctx, key).Result()
		if existsErr != nil {
			return nil, fmt.Errorf("get rate limit: check queue exists: %w", existsErr)
		}
		if exists == 0 {
			return nil, publicqueue.ErrNotFound
		}
		// Queue exists, no rate limit configured.
		return nil, nil
	}

	if rateLimitJSON == "" {
		return nil, nil
	}

	return UnmarshalRateLimitParams(ctx, rateLimitJSON)
}
