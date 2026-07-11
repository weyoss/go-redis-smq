/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package ns

import "errors"

// Sentinel errors.
var (
	ErrNotFound     = errors.New("namespace not found")
	ErrInvalidName  = errors.New("invalid namespace name")
	ErrNameRequired = errors.New("namespace name is required")
	ErrNotEmpty     = errors.New("namespace is not empty")
)
