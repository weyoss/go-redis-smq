/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package testutil

import (
	"context"
	"testing"

	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/redis"
)

// Setup prepares the test environment by flushing Redis and resetting config.
func Setup(tb testing.TB) context.Context {
	tb.Helper()
	ctx := context.Background()
	redis.Client().FlushAll(ctx)
	if err := internalconfig.Reload(ctx); err != nil {
		tb.Fatalf("reload config: %v", err)
	}
	return ctx
}
