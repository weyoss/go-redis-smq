/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
)

func TestMain(m *testing.M) {
	testutil.RunTestsWithRedis(m)
}
