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
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq"
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	internaleventbus "github.com/weyoss/go-redis-smq/internal/eventbus"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// initRedisSMQ initialises RedisSMQ with a single-node Redis client.
// It returns the context and registers cleanup functions to ensure
// RedisSMQ is shut down and the client is closed after each test.
//
// Cleanup registration order matters: cleanups run LIFO, so Shutdown must
// be registered AFTER client.Close to run BEFORE it. This is the opposite
// of the naive reading — the comment here is what the code does.
func initRedisSMQ(t *testing.T) (context.Context, *goredis.Client) {
	t.Helper()

	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})

	if err := redissmq.Init(ctx, client); err != nil {
		_ = client.Close()
		t.Fatalf("init redissmq: %v", err)
	}

	// LIFO: client.Close was registered last, so it runs first. The stale
	// comment in the original file claimed the opposite; this version
	// intentionally runs Shutdown last (so all Redis-touching teardown
	// inside it has a live client to talk to). Since these tests never
	// start producers or consumers, Shutdown's Redis operations are
	// no-ops anyway — but keeping the ordering correct is defensive.
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(redissmq.Shutdown)

	// Flush Redis and reset config for test isolation.
	testutil.Setup(t)

	return ctx, client
}

// assertPanics asserts that fn panics. Used to verify that runtime state
// was cleared (calling internalredis.Client after Close panics; calling
// internaleventbus.System after ShutdownSystem panics).
func assertPanics(t *testing.T, msg string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: function did not panic — state was not rolled back", msg)
		}
	}()
	fn()
}

// assertNotPanics asserts that fn does NOT panic. Used to verify that
// runtime state was preserved (calling internalconfig.Get after Init #2
// should not panic even if Init #1's watcher fires).
func assertNotPanics(t *testing.T, msg string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s: unexpected panic: %v", msg, r)
		}
	}()
	fn()
}

// ── Happy path ──

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
	// Should not panic even if Init was never called. With the idempotent
	// shutdownLocked, this now performs a full no-op unwind (every step is
	// nil-safe) rather than early-returning.
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
// shutdownIfCurrent(watchedGen) — which sees initialized==false and
// returns — and exits. Zero leak per cycle.
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

// TestSystem_OldInitWatcherDoesNotTeardownNewInit verifies that a watcher
// from an earlier Init cannot tear down a later Init's runtime.
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

	// Init #2 with a fresh context.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	if err := redissmq.Init(ctx2, client); err != nil {
		t.Fatalf("init #2: %v", err)
	}
	t.Cleanup(redissmq.Shutdown)

	// Sanity: Init #2 is running.
	_ = internalconfig.Get()

	// Cancel ctx1 — the OUTER context of the FIRST Init. Pre-fix, this
	// fires the leaked watcher from Init #1, which calls Shutdown() and
	// tears down Init #2. Post-fix, Init #1's watcher already exited when
	// Init #1's Shutdown cancelled its systemCtx; cancel1 is a no-op from
	// the watcher's perspective.
	cancel1()
	time.Sleep(200 * time.Millisecond)

	// If Init #2 was torn down, config.Get panics.
	assertNotPanics(t, "internalconfig.Get after cancel1 (Init #2 should still be live)",
		func() { _ = internalconfig.Get() })
}

// ── Init partial-failure cleanup ──

// TestSystem_InitPartialFailureRollsBack verifies that a failed Init cleans
// up everything it already initialized, leaving the runtime ready for a
// clean retry.
//
// Failure mode exercised: seed a config hash with version=5 and invalid
// data. loadOrCreateLocked reads the hash, Decode fails (invalid data),
// falls through to createDefaultsLocked, which calls save-config.lua with
// expectedVersion=0. The script sees currentVersion=5 != 0 and returns
// VERSION_MISMATCH — surfacing as ErrVersionMismatch through
// internalconfig.Init.
func TestSystem_InitPartialFailureRollsBack(t *testing.T) {
	// Reset state from any previous test in this package. Shutdown is now
	// unconditional (idempotent), so it fully unwinds any lingering state.
	redissmq.Shutdown()

	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()

	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	// Seed a config hash that will make internalconfig.Init fail after
	// internalredis.Init and internaleventbus.InitSystem have both succeeded.
	cfgKey := keys.System{}.Config()
	if err := client.HSet(ctx, cfgKey, "version", "5").Err(); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := client.HSet(ctx, cfgKey, "data", "{not valid json").Err(); err != nil {
		t.Fatalf("seed data: %v", err)
	}

	err := redissmq.Init(ctx, client)
	if err == nil {
		redissmq.Shutdown()
		t.Fatal("expected Init to fail: invalid config data with non-zero version " +
			"forces VERSION_MISMATCH during defaults creation")
	}
	if !strings.Contains(err.Error(), "config init failed") {
		t.Errorf("error should mention 'config init failed': %v", err)
	}

	// Rollback assertion 1: internalredis.Client() must panic. Pre-fix, it
	// still returned the stored client (state was never cleaned up).
	assertPanics(t, "internalredis.Client after rollback (did Init call shutdownLocked?)",
		func() { _ = internalredis.Client() })

	// Rollback assertion 2: internalredis.Eval must also panic (scriptMgr
	// was cleared).
	assertPanics(t, "internalredis.Eval after rollback",
		func() { _, _ = internalredis.Eval(ctx, "whatever", []string{}, []interface{}{}...) })

	// Rollback assertion 3: internaleventbus.System() must panic. Pre-fix,
	// it returned the still-running bus from the failed Init.
	assertPanics(t, "internaleventbus.System after rollback (did Init call shutdownLocked?)",
		func() { _ = internaleventbus.System() })

	// Recovery: remove the broken config and re-Init. This proves the
	// runtime is in a fully clean state, not just a state where Shutdown
	// happens to be safe.
	if err := client.Del(ctx, cfgKey).Err(); err != nil {
		t.Fatalf("del broken config: %v", err)
	}
	if err := redissmq.Init(ctx, client); err != nil {
		t.Fatalf("Init after rollback: %v", err)
	}
	t.Cleanup(redissmq.Shutdown)

	// Sanity: the retry actually initialized the config manager.
	_ = internalconfig.Get()
}

// TestSystem_InitPartialFailureRetryWithDifferentClient verifies that a
// retry with a NEW client is honored — not silently substituted with the
// client from the failed attempt (which is what internalredis.Init's fast
// path would do if the failed attempt's state weren't rolled back).
func TestSystem_InitPartialFailureRetryWithDifferentClient(t *testing.T) {
	redissmq.Shutdown()

	ctx := context.Background()

	client1 := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client1.Close() })

	if err := client1.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	cfgKey := keys.System{}.Config()
	client1.HSet(ctx, cfgKey, "version", "5")
	client1.HSet(ctx, cfgKey, "data", "{invalid")

	// First Init fails at config stage.
	if err := redissmq.Init(ctx, client1); err == nil {
		redissmq.Shutdown()
		t.Fatal("expected first Init to fail")
	}

	// Clear broken config, then retry with a DIFFERENT client.
	if err := client1.Del(ctx, cfgKey).Err(); err != nil {
		t.Fatalf("del config: %v", err)
	}

	client2 := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client2.Close() })

	if err := redissmq.Init(ctx, client2); err != nil {
		t.Fatalf("retry Init with new client: %v", err)
	}
	t.Cleanup(redissmq.Shutdown)

	// Verify internalredis.Client() points at client2, not client1.
	got := internalredis.Client()
	if got != client2 {
		t.Fatalf("internalredis.Client() = %p, want client2 (%p); "+
			"pre-fix, the failed Init's client1 was silently reused",
			got, client2)
	}
}

// TestSystem_InitRetryReopensLogger verifies that a retry after a failed
// Init runs with logging enabled, even though the failure path closed the
// logger via shutdownLocked's logger.Shutdown() call.
//
// Without logger.Init() at the top of Init, a retry would run silently —
// the user's Logger.Enabled=true config would have no effect until the
// process restarted.
func TestSystem_InitRetryReopensLogger(t *testing.T) {
	redissmq.Shutdown()

	ctx := context.Background()
	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	// Seed a broken config so Init #1 fails at the config stage.
	cfgKey := keys.System{}.Config()
	client.HSet(ctx, cfgKey, "version", "5")
	client.HSet(ctx, cfgKey, "data", "{invalid")

	if err := redissmq.Init(ctx, client); err == nil {
		redissmq.Shutdown()
		t.Fatal("expected first Init to fail")
	}
	// At this point shutdownLocked has run inside Init, closing the logger.

	// Clear the broken config and retry.
	if err := client.Del(ctx, cfgKey).Err(); err != nil {
		t.Fatalf("del broken config: %v", err)
	}

	if err := redissmq.Init(ctx, client); err != nil {
		t.Fatalf("retry Init: %v", err)
	}
	t.Cleanup(redissmq.Shutdown)

	// Force a log call from the internal logger with a config that enables
	// it. If logger.Init() was not called at the top of Init, the closed
	// flag is still true and this log call is silently dropped.
	cfg := internalconfig.Get()
	cfg.Logger.Enabled = true
	cfg.Logger.Options.LogLevel = 0
	if _, err := internalconfig.Save(ctx, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	// Capture the next log line from a fresh logger.
	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "retry-test")
	log.Debug("reopen-probe")

	if !strings.Contains(buf.String(), "reopen-probe") {
		t.Fatalf("logger did not emit after retry; "+
			"Init likely failed to call logger.Init() after a prior failure. "+
			"output was %q", buf.String())
	}
}

// Scenario: Full stack — producers, consumers, purge worker all active —
// shuts down cleanly under -race without leaking goroutines.
func TestSystem_FullLifecycleNoGoroutineLeak(t *testing.T) {
	redissmq.Shutdown()

	client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}

	// Warm-up cycle.
	if err := redissmq.Init(ctx, client); err != nil {
		t.Fatalf("warm-up init: %v", err)
	}
	redissmq.Shutdown()
	time.Sleep(300 * time.Millisecond)

	baseline := runtime.NumGoroutine()

	// Full-stack lifecycle.
	for i := 0; i < 3; i++ {
		if err := redissmq.Init(ctx, client); err != nil {
			t.Fatalf("cycle %d init: %v", i, err)
		}

		params := queue.MustQueueParams(fmt.Sprintf("test-c9-full-%d", i))
		if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
			t.Fatalf("create queue: %v", err)
		}

		cons := redissmq.NewConsumer(consumer.WithHeartbeatTTL(1 * time.Second))
		cons.Consume(params, func(ctx context.Context, m *message.Transferable) error { return nil })
		if err := cons.Run(ctx); err != nil {
			t.Fatalf("run consumer: %v", err)
		}

		prod := redissmq.NewProducer()
		if err := prod.Run(ctx); err != nil {
			t.Fatalf("run producer: %v", err)
		}
		prod.Produce(ctx, message.New().SetBody("msg").SetQueue(params))

		time.Sleep(200 * time.Millisecond)

		redissmq.Shutdown()
	}

	time.Sleep(500 * time.Millisecond)

	after := runtime.NumGoroutine()
	delta := after - baseline

	if delta > 5 {
		t.Fatalf("goroutine leak after full lifecycle cycles: "+
			"baseline=%d after=%d delta=+%d (expected ≤ +5)",
			baseline, after, delta)
	}
	t.Logf("goroutine delta after full lifecycle: %+d (baseline %d)", delta, baseline)
}
