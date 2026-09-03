/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package queue provides internal implementations for RedisSMQ queue
// operations. This file contains codecs used for queue Params and Props.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/ratelimit"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Codec handles serialisation for queue Params and Props.
// It centralises all queue‑related encoding and decoding.
type Codec struct {
	rateLimitCodec *ratelimit.Codec
}

// NewCodec creates a new Codec with default dependencies.
func NewCodec() *Codec {
	return &Codec{
		rateLimitCodec: ratelimit.NewCodec(),
	}
}

// EncodeParams serialises Params to a JSON string for Redis set storage.
func (c *Codec) EncodeParams(_ context.Context, params *publicqueue.Params) (string, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encode queue params: %w", err)
	}
	return string(data), nil
}

// DecodeParams deserialises a JSON string from a Redis set back to Params.
func (c *Codec) DecodeParams(_ context.Context, data string) (*publicqueue.Params, error) {
	var params publicqueue.Params
	if err := json.Unmarshal([]byte(data), &params); err != nil {
		return nil, fmt.Errorf("decode queue params: %w", err)
	}
	if params.Name() == "" || params.NS() == "" {
		return nil, fmt.Errorf("decode queue params: invalid format")
	}
	return &params, nil
}

// DecodeParamsSlice decodes multiple JSON-encoded queue params.
// Malformed entries are silently skipped.
func (c *Codec) DecodeParamsSlice(ctx context.Context, members []string) ([]publicqueue.Params, error) {
	params := make([]publicqueue.Params, 0, len(members))
	for _, member := range members {
		p, err := c.DecodeParams(ctx, member)
		if err != nil {
			continue
		}
		params = append(params, *p)
	}
	return params, nil
}

// DecodeProps deserialises a Redis hash map back to Props.
func (c *Codec) DecodeProps(ctx context.Context, hash map[string]string) (*publicqueue.Props, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode queue props: empty hash")
	}

	props := &publicqueue.Props{}

	// Queue type
	if v, ok := hash[schema.Type.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.Type = publicqueue.Type(n)
	}

	// Delivery model
	if v, ok := hash[schema.DeliveryModel.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.DeliveryModel = publicqueue.DeliveryModel(n)
	}

	// Message counters
	props.MessagesCount = parseInt64Field(hash, schema.MessagesCount.Key())
	props.ScheduledMessagesCount = parseInt64Field(hash, schema.ScheduledMessagesCount.Key())
	props.PendingMessagesCount = parseInt64Field(hash, schema.PendingMessagesCount.Key())
	props.ProcessingMessagesCount = parseInt64Field(hash, schema.ProcessingMessagesCount.Key())
	props.AcknowledgedMessagesCount = parseInt64Field(hash, schema.AcknowledgedMessagesCount.Key())
	props.DeadLetteredMessagesCount = parseInt64Field(hash, schema.DeadLetteredMessagesCount.Key())
	props.DelayedMessagesCount = parseInt64Field(hash, schema.DelayedMessagesCount.Key())
	props.RequeuedMessagesCount = parseInt64Field(hash, schema.RequeuedMessagesCount.Key())

	// Operational state
	if v, ok := hash[schema.OperationalState.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.OperationalState = publicqueue.State(n)
	}

	// Rate limit - JSON encoded in hash field
	if v, ok := hash[schema.RateLimit.Key()]; ok && v != "" {
		rl, err := ratelimit.UnmarshalRateLimitParams(ctx, v)
		if err == nil && rl != nil {
			props.RateLimit = rl
		}
	}

	// Last state change timestamp (Unix milliseconds)
	if v, ok := hash[schema.LastStateChangeAt.Key()]; ok {
		ms, _ := strconv.ParseInt(v, 10, 64)
		props.LastStateChangeAt = time.UnixMilli(ms)
	}

	// Lock ID
	if v, ok := hash[schema.LockID.Key()]; ok {
		props.LockID = v
	}

	return props, nil
}

// parseInt64Field parses an int64 value from a hash map field.
// Returns 0 if the field is missing or invalid.
func parseInt64Field(hash map[string]string, field string) int64 {
	v, ok := hash[field]
	if !ok {
		return 0
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}
