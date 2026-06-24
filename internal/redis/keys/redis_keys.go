/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package keys

import (
	"strings"
)

const (
	version = "10"
	sep     = ":"
	prefix  = "redis-smq" + sep + version
)

// Key builds a Redis key with the standard prefix
func Key(parts ...string) string {
	if len(parts) == 0 {
		return prefix
	}

	var builder strings.Builder
	builder.WriteString(prefix)

	for _, part := range parts {
		builder.WriteString(sep)
		builder.WriteString(part)
	}

	return builder.String()
}
