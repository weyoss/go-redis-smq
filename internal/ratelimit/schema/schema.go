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

// RateLimitField identifies rate limit fields in Redis hash storage.
// Used for the dedicated rate limit hash key (separate from queue properties).
type RateLimitField int

const (
	RateLimitFieldLimit    RateLimitField = iota // 0 - Maximum messages allowed
	RateLimitFieldInterval                       // 1 - Time window in milliseconds
)

// Key returns the string representation for Redis hash field access.
func (f RateLimitField) Key() string { return strconv.Itoa(int(f)) }

// Int returns the integer representation.
func (f RateLimitField) Int() int { return int(f) }
