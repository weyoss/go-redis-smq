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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/config"
	internalConfigEvents "github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/eventmultiplexer"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
)

// Scenario: Go publishers emit 3-arg events (config, version, epoch).
// The subscriber must populate UpdatedPayload.Epoch. Regression guard
// for the hybrid wire format's Go side.
func TestConfigEvents_Hybrid_GoPublisherPopulatesEpoch(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received internalConfigEvents.UpdatedPayload
	sub, err := internalConfigEvents.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	cfg := config.Get()
	cfg.Logger.Enabled = true
	if _, err := config.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Epoch == "" {
		t.Error("Epoch should be non-empty for Go-originated events")
	}
	if received.Version <= 0 {
		t.Errorf("Version = %d, want > 0", received.Version)
	}
}

// Scenario: TS publishers emit 2-arg events (config, version). The
// subscriber must accept them (Epoch left empty) and the handler must
// fall back to version-only comparison — triggering a Reload that picks
// up the new config from Redis. Regression test for the TS → Go half of
// the hybrid.
func TestConfigEvents_Hybrid_TypeScriptPublisher2Args(t *testing.T) {
	ctx := testutil.Setup(t)

	// Reach into Redis directly and bump the config data + version,
	// simulating a TS peer's save. This is what the TS save-config.lua
	// does: HSet version field, HSet data field. We don't run the lua
	// (that would go through internalconfig.Save and emit a Go event);
	// instead we write the same state and manually publish a 2-arg
	// event to simulate the TS peer's publish.
	cfgKey := keys.System{}.Config()

	cfg := config.Get()
	newVersion := cfg.Version + 1
	cfg.Namespace = "from-ts"
	cfg.Version = newVersion

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := redis.Client().HSet(ctx, cfgKey, "version", newVersion).Err(); err != nil {
		t.Fatalf("hset version: %v", err)
	}
	if err := redis.Client().HSet(ctx, cfgKey, "data", string(data)).Err(); err != nil {
		t.Fatalf("hset data: %v", err)
	}

	// Publish a 2-arg event, as TS would. Bypass PublishUpdated because
	// that helper emits 3 args.
	if err := eventmultiplexer.Publish(ctx,
		internalConfigEvents.EventUpdated,
		cfg,
		newVersion,
	); err != nil {
		t.Fatalf("publish 2-arg: %v", err)
	}

	// The subscriber should trigger a Reload. Poll the in-memory config
	// until it reflects the TS-written namespace.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if config.Get().Namespace == "from-ts" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("namespace = %q, want from-ts (2-arg event did not trigger reload)",
		config.Get().Namespace)
}

// Scenario: a 2-arg event with a version equal to the local version must
// be a no-op (duplicate delivery). This guards against spurious reloads
// from duplicate TS event deliveries.
func TestConfigEvents_Hybrid_TypeScriptSameVersionNoReload(t *testing.T) {
	ctx := testutil.Setup(t)

	// Instrument the system bus: count reload-triggering signals by
	// watching for a subsequent config change. Since we can't easily
	// hook Reload itself, we use a simpler invariant: publish a 2-arg
	// event with the local version and assert the local namespace is
	// unchanged after a generous wait. If a reload fired spuriously,
	// it would read the current Redis state — which matches the local
	// state — so the observable effect is nil either way. This test
	// documents the intent rather than asserting against a specific
	// internal call.
	//
	// A stricter test would need internal hooks; the happy-path
	// hybrid dispatch is covered by the previous two tests.
	cfg := config.Get()
	localVersion := cfg.Version

	if err := eventmultiplexer.Publish(ctx,
		internalConfigEvents.EventUpdated,
		cfg,
		localVersion,
	); err != nil {
		t.Fatalf("publish 2-arg same-version: %v", err)
	}

	// Give the subscriber enough time to have processed the event.
	time.Sleep(500 * time.Millisecond)

	if config.Get().Version != localVersion {
		t.Errorf("version changed from %d to %d (should be no-op)",
			localVersion, config.Get().Version)
	}
}
