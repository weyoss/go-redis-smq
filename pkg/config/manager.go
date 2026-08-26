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

import "context"

// Manager is the public interface for runtime configuration management.
//
// The concrete implementation is provided by the root redissmq package and
// can be obtained via redissmq.NewConfigManager().
type Manager interface {
	// Init loads or creates the configuration singleton.
	Init(ctx context.Context) error

	// Get returns the current configuration.
	Get() *Config

	// Save persists the given configuration and returns the new version.
	Save(ctx context.Context, cfg *Config) (int, error)

	// Reset restores the configuration to factory defaults.
	Reset(ctx context.Context) error

	// Reload reloads the configuration from Redis.
	Reload(ctx context.Context) error

	// Close releases the configuration singleton.
	Close()
}
