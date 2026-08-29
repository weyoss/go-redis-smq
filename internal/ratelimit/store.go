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
	"fmt"

	qSchema "github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

type Store struct {
	codec *Codec
}

func NewStore() *Store {
	return &Store{
		codec: NewRateLimitCodec(),
	}
}

func (s *Store) Codec() *Codec {
	return s.codec
}

func (s *Store) Set(ctx context.Context, queueParams *publicqueue.Params, rl *publicqueue.RateLimitParams) error {
	queueKeys := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	rateLimitJSON, err := s.codec.EncodeJSON(ctx, rl)
	if err != nil {
		return fmt.Errorf("set rate limit: encode: %w", err)
	}

	luaKeys := []string{queueKeys.Properties()}

	argv := []interface{}{
		qSchema.QueueFieldRateLimit.Key(),
		rateLimitJSON,
		qSchema.QueueFieldOperationalState.Key(),
		publicqueue.StateLocked.String(),
		qSchema.QueueFieldLockID.Key(),
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
		qSchema.QueueFieldRateLimit.Key(),
		qSchema.QueueFieldOperationalState.Key(),
		publicqueue.StateLocked.String(),
		qSchema.QueueFieldLockID.Key(),
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

func (s *Store) Get(ctx context.Context, queueParams *publicqueue.Params) (*publicqueue.RateLimitParams, error) {
	key := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}.Properties()

	rateLimitJSON, err := redisClient.LoadHashField(ctx, key,
		qSchema.QueueFieldRateLimit.Key(), "rate limit")
	if err != nil {
		return nil, nil
	}

	if rateLimitJSON == "" {
		return nil, nil
	}

	return s.codec.DecodeJSON(ctx, rateLimitJSON)
}
