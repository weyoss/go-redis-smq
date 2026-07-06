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

	"github.com/weyoss/go-redis-smq/internal/codec"
	"github.com/weyoss/go-redis-smq/pkg/config/cfg"
)

const (
	ConfigFieldVersion = "version"
	ConfigFieldData    = "data"
)

type Codec struct{}

func NewCodec() *Codec {
	return &Codec{}
}

func (c *Codec) EncodeHash(ctx context.Context, cfg *cfg.Config) (map[string]interface{}, error) {
	if cfg == nil {
		return nil, codec.NewEncodingError("config", "nil", codec.ErrInvalidFormat)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, codec.NewEncodingError("config", "json", err)
	}

	return map[string]interface{}{
		ConfigFieldVersion: cfg.Version,
		ConfigFieldData:    string(data),
	}, nil
}

func (c *Codec) DecodeHash(ctx context.Context, hash map[string]string) (*cfg.Config, error) {
	if len(hash) == 0 {
		return nil, codec.NewDecodingError("config", "empty hash", codec.ErrInvalidFormat)
	}

	data, ok := hash[ConfigFieldData]
	if !ok || data == "" {
		return nil, codec.NewDecodingError("config", "missing data field", codec.ErrInvalidFormat)
	}

	var cfg cfg.Config
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, codec.NewDecodingError("config", "json", err)
	}

	if v, ok := hash[ConfigFieldVersion]; ok {
		fmt.Sscanf(v, "%d", &cfg.Version)
	}

	return &cfg, nil
}

var _ codec.HashCodec[*cfg.Config] = (*Codec)(nil)
