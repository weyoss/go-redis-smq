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

	"github.com/weyoss/go-redis-smq/internal/exchange/schema"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
)

// Codec handles serialization for exchange Params and Props.
// It centralises all exchange‑related encoding and decoding.
type Codec struct{}

// NewCodec creates a new Codec.
func NewCodec() *Codec {
	return &Codec{}
}

// EncodeParams serializes Params to a JSON string for Redis set storage.
func (c *Codec) EncodeParams(_ context.Context, params *pubexchange.Params) (string, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("encode exchange params: %w", err)
	}
	return string(data), nil
}

// DecodeParams deserializes a JSON string from a Redis set back to Params.
func (c *Codec) DecodeParams(_ context.Context, data string) (*pubexchange.Params, error) {
	var params pubexchange.Params
	if err := json.Unmarshal([]byte(data), &params); err != nil {
		return nil, fmt.Errorf("decode exchange params: %w", err)
	}
	if params.Name() == "" || params.Namespace() == "" {
		return nil, fmt.Errorf("decode exchange params: invalid format")
	}
	return &params, nil
}

// EncodeProps serializes Props to a Redis hash map.
func (c *Codec) EncodeProps(_ context.Context, props *pubexchange.Props) (map[string]string, error) {
	if props == nil {
		return nil, fmt.Errorf("encode exchange props: nil")
	}
	return map[string]string{
		schema.Type.Key():   strconv.Itoa(props.Type.Int()),
		schema.Policy.Key(): strconv.Itoa(props.Policy.Int()),
	}, nil
}

// DecodeProps deserializes a Redis hash map back to Props.
func (c *Codec) DecodeProps(_ context.Context, hash map[string]string) (*pubexchange.Props, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode exchange props: empty hash")
	}

	props := &pubexchange.Props{}

	if v, ok := hash[schema.Type.Key()]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("decode exchange props type: %w", err)
		}
		props.Type = pubexchange.Type(n)
	}

	if v, ok := hash[schema.Policy.Key()]; ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("decode exchange props policy: %w", err)
		}
		props.Policy = pubexchange.Policy(n)
	}

	return props, nil
}
