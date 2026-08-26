// pkg/config/config.go
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

import "sync/atomic"

// currentConfig holds the most recent configuration snapshot.
// It is updated by the internal configuration manager whenever the
// configuration is loaded, saved, reset, or reloaded.
var currentConfig atomic.Pointer[Config]

func init() {
	// Start with factory defaults until the real configuration is loaded.
	currentConfig.Store(DefaultConfig())
}

// Set replaces the current in‑memory configuration snapshot.
//
// It is intended to be called only by the internal configuration manager
// (from the root redissmq package) to keep public packages synchronised
// with the latest configuration.
func Set(c *Config) {
	if c != nil {
		currentConfig.Store(c)
	}
}

// Get returns the current configuration snapshot.
//
// It may be nil if the configuration has not yet been initialised; callers
// should handle that case if necessary.
func Get() *Config {
	return currentConfig.Load()
}
