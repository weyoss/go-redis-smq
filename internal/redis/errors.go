/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package redis

import "errors"

var (
	// ErrUnexpectedScriptReply indicates a Lua script returned an unexpected value type.
	ErrUnexpectedScriptReply = errors.New("unexpected script reply")
)
