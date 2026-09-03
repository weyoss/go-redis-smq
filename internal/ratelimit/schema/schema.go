/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package schema

import "strconv"

// Field identifies rate limit fields in Redis hash storage.
// Used for the dedicated rate limit hash key (separate from queue properties).
type Field int

const (
	Limit    Field = iota // 0 - Maximum messages allowed
	Interval              // 1 - Time window in milliseconds
)

// Key returns the string representation for Redis hash field access.
func (f Field) Key() string { return strconv.Itoa(int(f)) }

// Int returns the integer representation.
func (f Field) Int() int { return int(f) }
