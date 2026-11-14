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

import (
	"fmt"
)

// String extracts a string from a Redis Lua script reply.
// Returns an error if the reply is not a string.
func String(reply interface{}) (string, error) {
	s, ok := reply.(string)
	if !ok {
		return "", fmt.Errorf("%w: expected string, got %T", ErrUnexpectedScriptReply, reply)
	}
	return s, nil
}

// Int64 extracts an int64 from a Redis Lua script reply.
// Returns an error if the reply is not an int64.
func Int64(reply interface{}) (int64, error) {
	n, ok := reply.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: expected int64, got %T", ErrUnexpectedScriptReply, reply)
	}
	return n, nil
}
