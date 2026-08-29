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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/codec"
	"github.com/weyoss/go-redis-smq/internal/ratelimit/schema"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Codec handles serialization of RateLimitParams to/from Redis.
//
// The rate limit can be stored in two formats:
//
// 1. Dedicated Redis hash (for atomic updates via Lua scripts):
//
//	hash["0"] = "100"     // RateLimitFieldLimit: max messages
//	hash["1"] = "60000"   // RateLimitFieldInterval: interval in milliseconds
//
// 2. JSON-encoded string (for storage within queue properties hash field "1"):
//
//	{"limit":100,"interval":60000}
//
// Note: The JSON format uses "interval" as milliseconds to match TypeScript.
type Codec struct{}

// NewRateLimitCodec creates a new RateLimit codec.
func NewRateLimitCodec() *Codec {
	return &Codec{}
}

// EncodeHash serializes RateLimitParams to a Redis hash map.
// Used for the dedicated rate limit hash key.
func (c *Codec) EncodeHash(_ context.Context, rl *publicqueue.RateLimitParams) (map[string]interface{}, error) {
	if rl == nil {
		return nil, codec.NewEncodingError("rate limit", "nil", codec.ErrInvalidFormat)
	}

	return map[string]interface{}{
		schema.RateLimitFieldLimit.Key():    strconv.Itoa(rl.Limit()),
		schema.RateLimitFieldInterval.Key(): strconv.FormatInt(rl.Interval().Milliseconds(), 10),
	}, nil
}

// DecodeHash deserializes a Redis hash map back to RateLimitParams.
// Used for the dedicated rate limit hash key.
func (c *Codec) DecodeHash(_ context.Context, hash map[string]string) (*publicqueue.RateLimitParams, error) {
	if len(hash) == 0 {
		return nil, nil // Empty hash means no rate limit
	}

	limit, _ := strconv.Atoi(hash[schema.RateLimitFieldLimit.Key()])
	intervalMs, _ := strconv.ParseInt(hash[schema.RateLimitFieldInterval.Key()], 10, 64)

	if limit <= 0 {
		return nil, nil // Invalid limit means no rate limit
	}

	interval := time.Duration(intervalMs) * time.Millisecond
	if interval < time.Second {
		interval = time.Second // Minimum 1 second interval
	}

	rl, err := publicqueue.NewRateLimitParams(limit, interval)
	if err != nil {
		return nil, codec.NewDecodingError("rate limit", fmt.Sprintf("limit=%d interval=%d", limit, intervalMs), err)
	}

	return rl, nil
}

// EncodeJSON serializes RateLimitParams to a JSON string.
// Used for storage within queue properties hash field "1".
//
// JSON format matches TypeScript IRateLimitParams:
//
//	{"limit":100,"interval":60000}
//
// Note: "interval" is in milliseconds for TypeScript compatibility.
func (c *Codec) EncodeJSON(_ context.Context, rl *publicqueue.RateLimitParams) (string, error) {
	if rl == nil {
		return "", nil
	}

	data, err := json.Marshal(rl)
	if err != nil {
		return "", codec.NewEncodingError("rate limit", "json", err)
	}
	return string(data), nil
}

// DecodeJSON deserializes a JSON string back to RateLimitParams.
// Used for reading from queue properties hash field "1".
func (c *Codec) DecodeJSON(_ context.Context, data string) (*publicqueue.RateLimitParams, error) {
	if data == "" {
		return nil, nil
	}

	var rl publicqueue.RateLimitParams
	if err := json.Unmarshal([]byte(data), &rl); err != nil {
		return nil, codec.NewDecodingError("rate limit", "json", err)
	}

	if err := rl.Validate(); err != nil {
		return nil, codec.NewDecodingError("rate limit", "validation", err)
	}

	return &rl, nil
}

// Compile-time interface checks
var (
	_ codec.HashCodec[*publicqueue.RateLimitParams] = (*Codec)(nil)
)
