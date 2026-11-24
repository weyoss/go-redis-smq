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
// configuration is loaded, saved, reset, or reloaded, and cleared when
// the manager is closed.
var currentConfig atomic.Pointer[Config]

func init() {
	// Start with factory defaults until the real configuration is loaded.
	// This makes config.Get() safe before the first Init: callers like
	// queue.NewQueueParams that read Get().Namespace get the default
	// namespace instead of panicking.
	currentConfig.Store(DefaultConfig())
}

// Set replaces the current in-memory configuration snapshot.
//
// The input is cloned before storage, so a caller that retains its
// pointer and mutates it after Set has no effect on the stored
// snapshot (and no race with concurrent Get callers).
//
// It is intended to be called only by the internal configuration manager
// (from the root redissmq package) to keep public packages synchronised
// with the latest configuration. Passing nil clears the snapshot.
func Set(c *Config) {
	currentConfig.Store(c.Clone())
}

// Get returns an independent copy of the current configuration snapshot.
//
// The returned pointer is a private clone. Callers may mutate it freely
// (for example, `cfg := config.Get(); cfg.Logger.Enabled = true;
// config.Save(ctx, cfg)`) without affecting the stored snapshot or racing
// any other Get caller.
//
// Returns:
//   - non-nil, factory-default config: before the first Init (the
//     package init seeds DefaultConfig so pre-Init reads are safe).
//   - non-nil, live config: between Init and Close (the runtime window).
//   - nil: after Close, until the next Init. Callers running in the
//     shutdown window (loggers, deferred cleanups) must handle nil.
func Get() *Config {
	return currentConfig.Load().Clone()
}
