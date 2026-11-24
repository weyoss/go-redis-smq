/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 */

package testutil

import (
	"context"
	"log"
	"os"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	internalworker "github.com/weyoss/go-redis-smq/internal/worker"
)

// RunTestsWithRedis starts a Redis process, initializes RedisSMQ, runs tests,
// then shuts down RedisSMQ and stops Redis.
//
// The runtime is initialized with internalworker.TestIntervals() — every
// process-scoped worker cadence is 10-300x faster than production. This
// keeps the full test suite under Go's default 10-minute binary timeout
// even under -race, without weakening any test's semantics: deadlines
// scale with the intervals (see IntervalDeadline in helpers.go), so the
// tests observe the same number of ticks they always did — the ticks just
// happen faster.
//
// Production deployments use DefaultIntervals (via redissmq.Init with no
// option). The cadence choice here affects the test process only.
func RunTestsWithRedis(m *testing.M) {
	rp, err := StartRedisProcess()
	if err != nil {
		log.Printf("redis: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// rp.client is the *goredis.Client created inside StartRedisProcess.
	if err := redissmq.Init(ctx, rp.client,
		redissmq.WithWorkerIntervals(internalworker.TestIntervals()),
	); err != nil {
		rp.Close()
		log.Printf("redissmq.init: %v\n", err)
		os.Exit(1)
	}

	// Initialise the public user event bus for tests that use public
	// subscription functions.
	redissmq.InitUserEventBus(ctx)

	code := m.Run()

	redissmq.Shutdown()
	rp.Close()

	os.Exit(code)
}
