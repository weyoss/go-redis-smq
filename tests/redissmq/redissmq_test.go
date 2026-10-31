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
	"runtime"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
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

// TestSystem_InitShutdownNoGoroutineLeak verifies that Init/Shutdown cycles
// do not leak goroutines.
//
// Pre-fix: each Init spawned a watcher goroutine blocked on the caller's
// ctx.Done(). Shutdown() only cancelled systemCtx (a derived context) and
// never cancelled the caller's ctx — so the watcher never exited unless the
// caller itself cancelled. One leak per cycle.
//
// Post-fix: the watcher observes the systemCtx captured at Init time.
// Shutdown's systemStop() call closes that ctx, the watcher fires, calls
// Shutdown() (no-op — initialized is already false), and exits. Zero leak
// per cycle.
func TestSystem_InitShutdownNoGoroutineLeak(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	// Warm-up cycle: let one-time initialization (config manager, Redis
	// pools, event bus, purge worker's goroutines) settle before taking a
	// baseline. The second cycle is what we measure.
	if err := redissmq.Init(context.Background(), client); err != nil {
		t.Fatalf("warm-up init: %v", err)
	}
	redissmq.Shutdown()
	time.Sleep(300 * time.Millisecond)

	baseline := runtime.NumGoroutine()

	const cycles = 10
	for i := 0; i < cycles; i++ {
		if err := redissmq.Init(context.Background(), client); err != nil {
			t.Fatalf("cycle %d init: %v", i, err)
		}
		redissmq.Shutdown()
	}

	// Allow the runtime and Redis client to quiesce.
	time.Sleep(300 * time.Millisecond)

	after := runtime.NumGoroutine()
	delta := after - baseline

	// Slack of 3 covers Redis pool goroutines, scheduler housekeeping, and
	// other test-adjacent noise. A leak of 1 per cycle would show +10,
	// well above this threshold.
	if delta > 3 {
		t.Fatalf("goroutine leak across %d Init/Shutdown cycles: "+
			"baseline=%d after=%d delta=+%d (expected ≤ +3)",
			cycles, baseline, after, delta)
	}
	t.Logf("goroutine delta after %d cycles: %+d (baseline %d)", cycles, delta, baseline)
}

// TestSystem_OldInitWatcherDoesNotTeardownNewInit verifies that the watcher
// goroutine spawned by Init #N only ever shuts down state from Init #N.
//
// Pre-fix: the watcher observed the caller's ctx directly. If the caller
// cancelled that ctx AFTER Shutdown had already run and a NEW Init had
// started, the old watcher would fire and call Shutdown() — tearing down
// the new Init's state. This is not just a leak; it's a correctness bug:
// a stale context cancellation silently kills a live runtime.
//
// Post-fix: the watcher observes the systemCtx created by ITS OWN Init.
// That context is cancelled when that Init's Shutdown runs, so the watcher
// exits promptly after its own lifecycle ends. A subsequent Init's state
// is untouched.
func TestSystem_OldInitWatcherDoesNotTeardownNewInit(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	// Init #1 with a cancellable outer context.
	if err := redissmq.Init(ctx1, client); err != nil {
		t.Fatalf("init #1: %v", err)
	}
	redissmq.Shutdown()
	t.Cleanup(redissmq.Shutdown) // idempotent; safe final cleanup

	// Init #2 with a fresh context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := redissmq.Init(ctx2, client); err != nil {
		t.Fatalf("init #2: %v", err)
	}

	// Sanity: Init #2 is running. internalconfig.Get panics if the config
	// manager is nil — which it becomes after Shutdown's config.Close().
	_ = internalconfig.Get()

	// Cancel ctx1 — the OUTER context of the FIRST Init. Pre-fix, this fires
	// the leaked watcher from Init #1, which calls Shutdown() and tears down
	// Init #2's state. Post-fix, Init #1's watcher already exited when
	// Init #1's Shutdown cancelled systemStop; cancel1 is a no-op from the
	// watcher's perspective.
	cancel1()
	time.Sleep(200 * time.Millisecond)

	// Verify Init #2 is still alive. If the leaked watcher tore it down,
	// config.Get() panics.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Init #2 was torn down by the leaked watcher from Init #1: %v", r)
		}
	}()
	_ = internalconfig.Get()
}
