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

import "errors"

// Sentinel errors for configuration operations.
var (
	// ErrNotInitialized indicates that the configuration manager has not been
	// initialized.
	ErrNotInitialized = errors.New("config not initialized")

	// ErrVersionMismatch indicates that the configuration was modified by
	// another instance between reading and saving.
	ErrVersionMismatch = errors.New("config version mismatch — modified by another instance")

	// ErrInvalidConfig indicates that the configuration passed to Save is
	// invalid.
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrUnexpectedScriptReply indicates a Lua script returned an unexpected value type.
	ErrUnexpectedScriptReply = errors.New("unexpected script reply")
)
