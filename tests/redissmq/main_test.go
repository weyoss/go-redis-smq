/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package redissmq_test

import (
	"os"
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
)

var redisAddr string

func TestMain(m *testing.M) {
	rp, err := testutil.StartRedisProcess()
	if err != nil {
		os.Exit(1)
	}
	redisAddr = rp.Addr()

	code := m.Run()

	rp.Close()
	os.Exit(code)
}
