/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package cfg defines public configuration types and sentinel errors used by
// RedisSMQ's configuration API.
package cfg

import "errors"

// ErrNotInitialized indicates that the configuration system has not been
// initialized. Call config.Init() before using config.Get or config.Save.
var ErrNotInitialized = errors.New("config not initialized")

// ErrVersionMismatch indicates that the configuration was modified by another
// instance between reading and saving. The caller should re-read the current
// configuration and retry the operation.
var ErrVersionMismatch = errors.New("config version mismatch — modified by another instance")

// ErrInvalidConfig indicates that the configuration passed to Save is invalid.
var ErrInvalidConfig = errors.New("invalid configuration")
