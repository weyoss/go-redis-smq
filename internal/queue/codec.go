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
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/codec"
	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	"github.com/weyoss/go-redis-smq/internal/rate_limit"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// QueueParamsCodec handles serialization of QueueParams to/from Redis sets.
type QueueParamsCodec struct{}

// NewQueueParamsCodec creates a new QueueParams codec.
func NewQueueParamsCodec() *QueueParamsCodec {
	return &QueueParamsCodec{}
}

// EncodeSet serializes QueueParams to a JSON string for Redis set storage.
func (c *QueueParamsCodec) EncodeSet(ctx context.Context, params *q.QueueParams) (string, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return "", codec.NewEncodingError("queue params", params.String(), err)
	}
	return string(data), nil
}

// DecodeSet deserializes a JSON string from a Redis set back to QueueParams.
func (c *QueueParamsCodec) DecodeSet(ctx context.Context, data string) (*q.QueueParams, error) {
	var params q.QueueParams
	if err := json.Unmarshal([]byte(data), &params); err != nil {
		return nil, codec.NewDecodingError("queue params", data, err)
	}
	if params.Name() == "" || params.NS() == "" {
		return nil, codec.NewDecodingError("queue params", data, codec.ErrInvalidFormat)
	}
	return &params, nil
}

// QueuePropsCodec handles serialization of QueueProps to/from Redis hash.
type QueuePropsCodec struct {
	rateLimitCodec *rate_limit.RateLimitCodec
}

// NewQueuePropsCodec creates a new QueueProps codec.
func NewQueuePropsCodec() *QueuePropsCodec {
	return &QueuePropsCodec{
		rateLimitCodec: rate_limit.NewRateLimitCodec(),
	}
}

// EncodeHash serializes QueueProps to a Redis hash map.
func (c *QueuePropsCodec) EncodeHash(ctx context.Context, props *q.QueueProps) (map[string]interface{}, error) {
	if props == nil {
		return nil, codec.NewEncodingError("queue props", "nil", codec.ErrInvalidFormat)
	}

	hash := map[string]interface{}{
		schema.QueueFieldType.Key():                      props.Type.Int(),
		schema.QueueFieldDeliveryModel.Key():             props.DeliveryModel.Int(),
		schema.QueueFieldMessagesCount.Key():             strconv.FormatInt(props.MessagesCount, 10),
		schema.QueueFieldScheduledMessagesCount.Key():    strconv.FormatInt(props.ScheduledMessagesCount, 10),
		schema.QueueFieldPendingMessagesCount.Key():      strconv.FormatInt(props.PendingMessagesCount, 10),
		schema.QueueFieldProcessingMessagesCount.Key():   strconv.FormatInt(props.ProcessingMessagesCount, 10),
		schema.QueueFieldAcknowledgedMessagesCount.Key(): strconv.FormatInt(props.AcknowledgedMessagesCount, 10),
		schema.QueueFieldDeadLetteredMessagesCount.Key(): strconv.FormatInt(props.DeadLetteredMessagesCount, 10),
		schema.QueueFieldDelayedMessagesCount.Key():      strconv.FormatInt(props.DelayedMessagesCount, 10),
		schema.QueueFieldRequeuedMessagesCount.Key():     strconv.FormatInt(props.RequeuedMessagesCount, 10),
		schema.QueueFieldOperationalState.Key():          props.OperationalState.Int(),
	}

	// Rate limit is JSON-encoded within its hash field
	if props.RateLimit != nil {
		rlJSON, err := c.rateLimitCodec.EncodeJSON(ctx, props.RateLimit)
		if err != nil {
			return nil, codec.NewEncodingError("queue props", "rate limit", err)
		}
		if rlJSON != "" {
			hash[schema.QueueFieldRateLimit.Key()] = rlJSON
		}
	}

	// Timestamps stored as Unix milliseconds
	if !props.LastStateChangeAt.IsZero() {
		hash[schema.QueueFieldLastStateChangeAt.Key()] = strconv.FormatInt(props.LastStateChangeAt.UnixMilli(), 10)
	}

	// Lock ID only stored when present
	if props.LockID != "" {
		hash[schema.QueueFieldLockID.Key()] = props.LockID
	}

	return hash, nil
}

// DecodeHash deserializes a Redis hash map back to QueueProps.
func (c *QueuePropsCodec) DecodeHash(ctx context.Context, hash map[string]string) (*q.QueueProps, error) {
	if len(hash) == 0 {
		return nil, codec.NewDecodingError("queue props", "empty hash", codec.ErrInvalidFormat)
	}

	props := &q.QueueProps{}

	// Queue type
	if v, ok := hash[schema.QueueFieldType.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.Type = q.QueueType(n)
	}

	// Delivery model
	if v, ok := hash[schema.QueueFieldDeliveryModel.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.DeliveryModel = q.DeliveryModel(n)
	}

	// Message counters
	props.MessagesCount = parseInt64Field(hash, schema.QueueFieldMessagesCount.Key())
	props.ScheduledMessagesCount = parseInt64Field(hash, schema.QueueFieldScheduledMessagesCount.Key())
	props.PendingMessagesCount = parseInt64Field(hash, schema.QueueFieldPendingMessagesCount.Key())
	props.ProcessingMessagesCount = parseInt64Field(hash, schema.QueueFieldProcessingMessagesCount.Key())
	props.AcknowledgedMessagesCount = parseInt64Field(hash, schema.QueueFieldAcknowledgedMessagesCount.Key())
	props.DeadLetteredMessagesCount = parseInt64Field(hash, schema.QueueFieldDeadLetteredMessagesCount.Key())
	props.DelayedMessagesCount = parseInt64Field(hash, schema.QueueFieldDelayedMessagesCount.Key())
	props.RequeuedMessagesCount = parseInt64Field(hash, schema.QueueFieldRequeuedMessagesCount.Key())

	// Operational state
	if v, ok := hash[schema.QueueFieldOperationalState.Key()]; ok {
		n, _ := strconv.Atoi(v)
		props.OperationalState = q.QueueState(n)
	}

	// Rate limit - JSON encoded in hash field
	if v, ok := hash[schema.QueueFieldRateLimit.Key()]; ok && v != "" {
		rl, err := c.rateLimitCodec.DecodeJSON(ctx, v)
		if err == nil && rl != nil {
			props.RateLimit = rl
		}
	}

	// Last state change timestamp (Unix milliseconds)
	if v, ok := hash[schema.QueueFieldLastStateChangeAt.Key()]; ok {
		ms, _ := strconv.ParseInt(v, 10, 64)
		props.LastStateChangeAt = time.UnixMilli(ms)
	}

	// Lock ID
	if v, ok := hash[schema.QueueFieldLockID.Key()]; ok {
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

// Compile-time interface checks
var (
	_ codec.SetCodec[*q.QueueParams] = (*QueueParamsCodec)(nil)
	_ codec.HashCodec[*q.QueueProps] = (*QueuePropsCodec)(nil)
)
