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

// ExchangeField identifies exchange property fields in Redis hash storage.
type ExchangeField int

const (
	ExchangeFieldType   ExchangeField = iota // 0 - Exchange routing type
	ExchangeFieldPolicy                      // 1 - Queue policy constraint
)

// Key returns the string representation for Redis hash field access.
func (f ExchangeField) Key() string { return strconv.Itoa(int(f)) }

// Int returns the integer representation.
func (f ExchangeField) Int() int { return int(f) }
