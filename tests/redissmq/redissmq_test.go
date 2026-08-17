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
	"context"
	"testing"

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
)

func initRedisSMQ(t *testing.T) context.Context {
	t.Helper()

	ctx := context.Background()
	if err := redissmq.Init(ctx, redissmq.Config{Addr: redisAddr}); err != nil {
		t.Fatalf("init redissmq: %v", err)
	}

	// Automatically shut down after each test, even on failure.
	t.Cleanup(redissmq.Shutdown)

	// Flush Redis and reset config for test isolation.
	testutil.Setup(t)

	return ctx
}

func TestSystem_InitAndShutdown(t *testing.T) {
	initRedisSMQ(t)

	// Shutdown should complete without error. It is safe to call
	// multiple times because t.Cleanup will call it again later.
	redissmq.Shutdown()
}

func TestSystem_InitIdempotent(t *testing.T) {
	ctx := initRedisSMQ(t)

	// Second Init should be a no-op.
	if err := redissmq.Init(ctx, redissmq.Config{Addr: redisAddr}); err != nil {
		t.Fatalf("second init: %v", err)
	}
}

func TestSystem_ShutdownWithoutInit(t *testing.T) {
	// Should not panic even if Init was never called.
	redissmq.Shutdown()
}

func TestSystem_NewProducerAndConsumer(t *testing.T) {
	initRedisSMQ(t)

	producer := redissmq.NewProducer()
	if producer == nil {
		t.Fatal("NewProducer returned nil")
	}

	consumer := redissmq.NewConsumer()
	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
}
