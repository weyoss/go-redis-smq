/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/weyoss/go-redis-smq/internal/codec"
	"github.com/weyoss/go-redis-smq/internal/exchange/schema"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
)

// ExchangeParamsCodec handles serialization of Params to/from Redis sets.
type ExchangeParamsCodec struct{}

// NewExchangeParamsCodec creates a new Params codec.
func NewExchangeParamsCodec() *ExchangeParamsCodec {
	return &ExchangeParamsCodec{}
}

// EncodeSet serializes Params to a JSON string for Redis set storage.
func (c *ExchangeParamsCodec) EncodeSet(ctx context.Context, params *pubexchange.Params) (string, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return "", codec.NewEncodingError("exchange params", params.String(), err)
	}
	return string(data), nil
}

// DecodeSet deserializes a JSON string from a Redis set back to Params.
func (c *ExchangeParamsCodec) DecodeSet(ctx context.Context, data string) (*pubexchange.Params, error) {
	var params pubexchange.Params
	if err := json.Unmarshal([]byte(data), &params); err != nil {
		return nil, codec.NewDecodingError("exchange params", data, err)
	}
	if params.Name() == "" || params.Namespace() == "" {
		return nil, codec.NewDecodingError("exchange params", data, codec.ErrInvalidFormat)
	}
	return &params, nil
}

// ExchangePropsCodec handles serialization of ExchangeProps to/from Redis hash.
type ExchangePropsCodec struct{}

// NewExchangePropsCodec creates a new ExchangeProps codec.
func NewExchangePropsCodec() *ExchangePropsCodec {
	return &ExchangePropsCodec{}
}

// EncodeHash serializes ExchangeProps to a Redis hash map.
func (c *ExchangePropsCodec) EncodeHash(ctx context.Context, props *pubexchange.ExchangeProps) (map[string]interface{}, error) {
	if props == nil {
		return nil, codec.NewEncodingError("exchange props", "nil", codec.ErrInvalidFormat)
	}

	hash := map[string]interface{}{
		schema.ExchangeFieldType.Key():   strconv.Itoa(props.Type.Int()),
		schema.ExchangeFieldPolicy.Key(): strconv.Itoa(props.Policy.Int()),
	}

	return hash, nil
}

// DecodeHash deserializes a Redis hash map back to ExchangeProps.
func (c *ExchangePropsCodec) DecodeHash(ctx context.Context, hash map[string]string) (*pubexchange.ExchangeProps, error) {
	if len(hash) == 0 {
		return nil, codec.NewDecodingError("exchange props", "empty hash", codec.ErrInvalidFormat)
	}

	props := &pubexchange.ExchangeProps{}

	if v, ok := hash[schema.ExchangeFieldType.Key()]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, codec.NewDecodingError("exchange props", fmt.Sprintf("type=%s", v), err)
		}
		props.Type = pubexchange.ExchangeType(n)
	}

	if v, ok := hash[schema.ExchangeFieldPolicy.Key()]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, codec.NewDecodingError("exchange props", fmt.Sprintf("policy=%s", v), err)
		}
		props.Policy = pubexchange.ExchangePolicy(n)
	}

	return props, nil
}

// Compile-time interface checks
var (
	_ codec.SetCodec[*pubexchange.Params]         = (*ExchangeParamsCodec)(nil)
	_ codec.HashCodec[*pubexchange.ExchangeProps] = (*ExchangePropsCodec)(nil)
)
