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
	"log"
	"os"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
)

// RunTestsWithRedis starts a Redis process, initializes RedisSMQ, runs tests,
// then shuts down RedisSMQ and stops Redis.
func RunTestsWithRedis(m *testing.M) {
	// log.Println("testutil: RunTestsWithRedis START")
	// log.Println("testutil: starting Redis process...")

	rp, err := StartRedisProcess()
	if err != nil {
		log.Printf("redis: %v\n", err)
		os.Exit(1)
	}
	// log.Printf("testutil: Redis started on %s\n", rp.Addr())

	ctx := context.Background()
	// log.Println("testutil: initializing RedisSMQ...")

	// rp.client is the *goredis.Client created inside StartRedisProcess.
	if err := redissmq.Init(ctx, rp.client); err != nil {
		rp.Close()
		log.Printf("redissmq.init: %v\n", err)
		os.Exit(1)
	}
	// log.Println("testutil: RedisSMQ initialized")

	// Initialise the public user event bus for tests that use public
	// subscription functions.
	redissmq.InitUserEventBus(ctx)

	// log.Println("testutil: running tests...")
	code := m.Run()
	// log.Printf("testutil: tests finished with code %d\n", code)

	// log.Println("testutil: shutting down RedisSMQ...")
	redissmq.Shutdown()
	// log.Println("testutil: RedisSMQ shut down")

	// log.Println("testutil: stopping Redis...")
	rp.Close()
	// log.Println("testutil: Redis stopped")

	// log.Println("testutil: RunTestsWithRedis DONE")
	os.Exit(code)
}
