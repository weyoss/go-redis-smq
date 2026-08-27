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
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
)

// initRedisSMQ initialises RedisSMQ with a single-node Redis client.
// It returns the context and registers cleanup functions to ensure
// RedisSMQ is shut down and the client is closed after each test.
func initRedisSMQ(t *testing.T) (context.Context, *goredis.Client) {
	t.Helper()

	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})

	if err := redissmq.Init(ctx, client); err != nil {
		_ = client.Close()
		t.Fatalf("init redissmq: %v", err)
	}

	// Shutdown RedisSMQ first, then close the client.
	t.Cleanup(redissmq.Shutdown)
	t.Cleanup(func() { _ = client.Close() })

	// Flush Redis and reset config for test isolation.
	testutil.Setup(t)

	return ctx, client
}

func TestSystem_InitAndShutdown(t *testing.T) {
	initRedisSMQ(t)

	// Shutdown should complete without error. It is safe to call
	// multiple times because t.Cleanup will call it again later.
	redissmq.Shutdown()
}

func TestSystem_InitIdempotent(t *testing.T) {
	ctx, client := initRedisSMQ(t)

	// Second Init should be a no-op.
	if err := redissmq.Init(ctx, client); err != nil {
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

// TestSystem_RejectsClusterClient ensures that a cluster client is
// rejected before any Redis interaction, even if RedisSMQ is already
// initialised with a single-node client.
func TestSystem_RejectsClusterClient(t *testing.T) {
	// Ensure RedisSMQ is already running with a valid client.
	initRedisSMQ(t)

	clusterClient := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs: []string{redisAddr},
	})
	defer clusterClient.Close()

	err := redissmq.Init(context.Background(), clusterClient)
	if err == nil {
		t.Fatal("expected error when passing a cluster client")
	}

	if !strings.Contains(err.Error(), "only *redis.Client is supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}
