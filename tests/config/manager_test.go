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
	"context"
	"encoding/json"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/testutil"
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
