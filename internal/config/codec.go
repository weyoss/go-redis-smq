/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/weyoss/go-redis-smq/pkg/config"
)

const (
	FieldVersion = "version"
	FieldData    = "data"
)

// Codec handles serialisation for Config to/from Redis hashes.
type Codec struct{}

// NewCodec creates a new Codec.
func NewCodec() *Codec {
	return &Codec{}
}

// Encode serialises Config to a Redis hash map.
func (c *Codec) Encode(_ context.Context, cfg *config.Config) (map[string]string, error) {
	if cfg == nil {
		return nil, fmt.Errorf("encode config: nil")
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}

	return map[string]string{
		FieldVersion: strconv.Itoa(cfg.Version),
		FieldData:    string(data),
	}, nil
}

// Decode deserialises a Redis hash map back to Config.
func (c *Codec) Decode(_ context.Context, hash map[string]string) (*config.Config, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode config: empty hash")
	}

	data, ok := hash[FieldData]
	if !ok || data == "" {
		return nil, fmt.Errorf("decode config: missing data field")
	}

	var cfg config.Config
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if v, ok := hash[FieldVersion]; ok {
		version, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("decode config version: %w", err)
		}
		cfg.Version = version
	}

	return &cfg, nil
}
