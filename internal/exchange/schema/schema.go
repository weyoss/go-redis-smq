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

// Field identifies exchange property fields in Redis hash storage.
type Field int

const (
	Type   Field = iota // 0 - Exchange routing type
	Policy              // 1 - Queue policy constraint
)

// Key returns the string representation for Redis hash field access.
func (f Field) Key() string { return strconv.Itoa(int(f)) }

// Int returns the integer representation.
func (f Field) Int() int { return int(f) }
