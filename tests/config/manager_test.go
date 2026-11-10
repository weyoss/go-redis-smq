/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

func TestManager_Init(t *testing.T) {
	ctx := testutil.Setup(t)

	manager := redissmq.NewConfigManager()

	// Init is idempotent and should succeed even after test setup already
	// initialised the configuration.
	if err := manager.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
}

func TestManager_Get(t *testing.T) {
	testutil.Setup(t)

	manager := redissmq.NewConfigManager()
	cfg := manager.Get()
	if cfg == nil {
		t.Fatal("Get returned nil")
	}
	if cfg.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", cfg.Namespace, "default")
	}
}

func TestManager_Save(t *testing.T) {
	ctx := testutil.Setup(t)

	manager := redissmq.NewConfigManager()
	cfg := manager.Get()
	oldVersion := cfg.Version

	cfg.Namespace = "save-test-ns"
	newVersion, err := manager.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if newVersion <= oldVersion {
		t.Errorf("version = %d, want > %d", newVersion, oldVersion)
	}

	// Verify the change persisted.
	updated := manager.Get()
	if updated.Namespace != "save-test-ns" {
		t.Errorf("namespace = %q, want %q", updated.Namespace, "save-test-ns")
	}
	if updated.Version != newVersion {
		t.Errorf("version = %d, want %d", updated.Version, newVersion)
	}
}

func TestManager_Reset(t *testing.T) {
	ctx := testutil.Setup(t)

	manager := redissmq.NewConfigManager()
	cfg := manager.Get()
	cfg.Namespace = "custom-ns"
	cfg.Logger.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	if _, err := manager.Save(ctx, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if err := manager.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	reset := manager.Get()
	if reset.Namespace != "default" {
		t.Errorf("namespace = %q, want default", reset.Namespace)
	}
	if reset.Logger.Enabled {
		t.Error("logger should be disabled after reset")
	}
	if reset.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be disabled after reset")
	}
}

func TestManager_Reload(t *testing.T) {
	ctx := testutil.Setup(t)

	manager := redissmq.NewConfigManager()
	cfg := manager.Get()
	cfg.Namespace = "initial-ns"
	if _, err := manager.Save(ctx, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Simulate an external change by directly updating the Redis config hash.
	externalCfg := pubconfig.DefaultConfig()
	externalCfg.Namespace = "external-change"
	data, err := json.Marshal(externalCfg)
	if err != nil {
		t.Fatalf("marshal external config: %v", err)
	}
	key := keys.System{}.Config()
	if err := redis.Client().HSet(ctx, key, "data", string(data)).Err(); err != nil {
		t.Fatalf("hset config data: %v", err)
	}

	if err := manager.Reload(ctx); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	reloaded := manager.Get()
	if reloaded.Namespace != "external-change" {
		t.Errorf("namespace = %q, want external-change", reloaded.Namespace)
	}
}

func TestManager_Close(t *testing.T) {
	testutil.Setup(t)

	manager := redissmq.NewConfigManager()

	// Close should not panic.
	manager.Close()

	// Re-initialize and ensure Get works again.
	ctx := context.Background()
	if err := manager.Init(ctx); err != nil {
		t.Fatalf("Init after Close: %v", err)
	}
	cfg := manager.Get()
	if cfg == nil {
		t.Fatal("Get returned nil after re-init")
	}
}

// Scenario: Manager.Close clears the public config snapshot (H8).
//
// Pre-fix, internalconfig.Close nilled m.instance but left
// pubconfig.Get() returning the last-live config. Any code running in
// the shutdown window (deferred cleanups, post-shutdown logging) that
// called NewQueueParams or NewExchangeParams would bake the
// pre-shutdown namespace into a params value, and any code that read
// Get().Namespace directly would silently operate against stale data.
//
// Post-fix, Close nils both m.instance and the public snapshot. The
// documented contract (pkg/config.Get) becomes: non-nil during the
// [Init..Close] runtime window, nil after Close. Logging is the one
// post-Close reader that legitimately runs and is handled by a nil
// guard in logger.currentConfig.
func TestManager_Close_ClearsSnapshot(t *testing.T) {
	ctx := testutil.Setup(t)

	// Sanity: after Setup's internalconfig.Init, Get returns non-nil.
	if cfg := pubconfig.Get(); cfg == nil {
		t.Fatal("pubconfig.Get() = nil after Init; expected non-nil")
	}

	// Close clears the snapshot.
	internalconfig.Close()

	if cfg := pubconfig.Get(); cfg != nil {
		t.Fatalf("pubconfig.Get() = %+v after Close; expected nil (H8)", cfg)
	}

	// Logging after Close must not panic. The logger's nil-guard returns
	// a disabled config so the message is silently dropped — that's the
	// correct behavior; what we're asserting is that no panic occurs.
	// Pre-fix (with H8 not applied but with the nil guard absent), this
	// would panic on c.Logger.Enabled dereference.
	var buf bytes.Buffer
	log := logger.NewWithWriter(&buf, "post-close")
	log.Info("post-close log message")
	if buf.Len() != 0 {
		t.Errorf("post-close log emitted output: %q (expected empty)", buf.String())
	}

	// Re-init restores the snapshot.
	if err := internalconfig.Init(ctx); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	if cfg := pubconfig.Get(); cfg == nil {
		t.Fatal("pubconfig.Get() = nil after re-Init; expected non-nil")
	}
}
