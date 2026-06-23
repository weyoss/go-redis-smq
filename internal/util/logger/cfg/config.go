/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// internal/util/logger/cfg/config.go

// Package cfg wires the application configuration into the logger's ConfigProvider.
package cfg

import (
	"log/slog"

	"github.com/weyoss/go-redis-smq/internal/util/logger"
	appconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// Provider returns a logger.ConfigProvider backed by the application config.
func Provider() logger.ConfigProvider {
	return adapter{}
}

type adapter struct{}

func (adapter) Get() logger.Config {
	c := appconfig.Get()
	level := slog.Level(c.Logger.Options.LogLevel)
	if level < slog.LevelDebug || level > slog.LevelError {
		level = slog.LevelInfo
	}
	return logger.Config{
		Enabled:          c.Logger.Enabled,
		Level:            level,
		Colorize:         c.Logger.Options.Colorize,
		IncludeTimestamp: c.Logger.Options.IncludeTimestamp,
	}
}
