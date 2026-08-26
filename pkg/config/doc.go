/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package config provides the public configuration types, errors, and the
// Manager interface for RedisSMQ.
//
// Configuration is stored centrally in Redis and shared across all connected
// instances. This package contains the data structures and the contract that
// the internal configuration manager implements. The concrete manager is
// created and owned by the root redissmq package; obtain it via
// redissmq.NewConfigManager().
//
// # Configuration Snapshot
//
// In addition to the Manager interface, this package maintains a thread-safe
// snapshot of the current configuration. It is updated automatically by the
// internal manager whenever the configuration is loaded, saved, reset, or
// reloaded. Public packages that need read-only access to configuration
// values (such as the default namespace) can retrieve the snapshot using
// Get() without depending on internal code.
//
//   - Get() returns the current snapshot.
//   - Set() is used by the internal manager to update the snapshot.
//   - DefaultNamespace() returns the default namespace for queue and exchange
//     operations.
//
// # Main Types
//
//   - Config: the top-level configuration structure.
//   - LoggerConfig: logging settings.
//   - MessageAudit: message audit trail settings.
//   - Manager: interface for runtime configuration operations.
//
// # Example
//
//	cfgManager := redissmq.NewConfigManager()
//	cfg := cfgManager.Get()
//	cfg.Logger.Enabled = true
//	version, err := cfgManager.Save(ctx, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Default Configuration
//
// Use DefaultConfig() to obtain the factory defaults. This is used internally
// when no configuration exists in Redis or when Reset() is called.
package config
