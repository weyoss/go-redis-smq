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
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/ratelimit/schema"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Codec handles serialization of RateLimitParams to/from Redis.
type Codec struct{}

// NewCodec creates a new Codec.
func NewCodec() *Codec {
	return &Codec{}
}

// Encode serializes RateLimitParams to a Redis hash map.
func (c *Codec) Encode(_ context.Context, rl *queue.RateLimitParams) (map[string]string, error) {
	if rl == nil {
		return nil, fmt.Errorf("encode rate limit: nil")
	}
	return map[string]string{
		schema.Limit.Key():    strconv.Itoa(rl.Limit()),
		schema.Interval.Key(): strconv.FormatInt(rl.Interval().Milliseconds(), 10),
	}, nil
}

// Decode deserializes a Redis hash map back to RateLimitParams.
func (c *Codec) Decode(_ context.Context, hash map[string]string) (*queue.RateLimitParams, error) {
	if len(hash) == 0 {
		return nil, nil // Empty hash means no rate limit
	}

	limit, _ := strconv.Atoi(hash[schema.Limit.Key()])
	intervalMs, _ := strconv.ParseInt(hash[schema.Interval.Key()], 10, 64)

	if limit <= 0 {
		return nil, nil // Invalid limit means no rate limit
	}

	interval := time.Duration(intervalMs) * time.Millisecond
	if interval < time.Second {
		interval = time.Second
	}

	rl, err := queue.NewRateLimitParams(limit, interval)
	if err != nil {
		return nil, fmt.Errorf("decode rate limit: %w", err)
	}
	return rl, nil
}
