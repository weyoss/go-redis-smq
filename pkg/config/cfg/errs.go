/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package cfg

import "errors"

var (
	// ErrNotInitialized indicates config.Init() has not been called.
	ErrNotInitialized = errors.New("config not initialized")

	// ErrVersionMismatch indicates the config was modified by another instance
	// between reading and saving. Caller should re-read and retry.
	ErrVersionMismatch = errors.New("config version mismatch — modified by another instance")

	// ErrInvalidConfig indicates the configuration passed to Save is invalid.
	ErrInvalidConfig = errors.New("invalid configuration")
)
