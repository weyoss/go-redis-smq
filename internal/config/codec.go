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

	"github.com/weyoss/go-redis-smq/internal/codec"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

const (
	ConfigFieldVersion = "version"
	ConfigFieldData    = "data"
)

type Codec struct{}

func NewCodec() *Codec {
	return &Codec{}
}

func (c *Codec) EncodeHash(_ context.Context, cfg *pubconfig.Config) (map[string]interface{}, error) {
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

func (c *Codec) DecodeHash(_ context.Context, hash map[string]string) (*pubconfig.Config, error) {
	if len(hash) == 0 {
		return nil, codec.NewDecodingError("config", "empty hash", codec.ErrInvalidFormat)
	}

	data, ok := hash[ConfigFieldData]
	if !ok || data == "" {
		return nil, codec.NewDecodingError("config", "missing data field", codec.ErrInvalidFormat)
	}

	var cfg pubconfig.Config
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, codec.NewDecodingError("config", "json", err)
	}

	if v, ok := hash[ConfigFieldVersion]; ok {
		version, err := strconv.Atoi(v)
		if err != nil {
			return nil, codec.NewDecodingError("config", fmt.Sprintf("version=%s", v), err)
		}
		cfg.Version = version
	}

	return &cfg, nil
}

var _ codec.HashCodec[*pubconfig.Config] = (*Codec)(nil)
