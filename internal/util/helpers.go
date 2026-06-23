/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package util

import "fmt"

func OrEmptyInt64(v *int64) string {
	if v != nil {
		return fmt.Sprintf("%d", *v)
	}
	return ""
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
