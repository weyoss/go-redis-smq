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
	"fmt"
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
)

// Scenario: Config update persists across saves
func TestEvent_UpdatePersists(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.Namespace = "event-persist"
	cfg.Logger.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.Namespace != "event-persist" {
		t.Errorf("namespace = %q, want %q", cfg2.Namespace, "event-persist")
	}
	if !cfg2.Logger.Enabled {
		t.Error("logger should be enabled")
	}
}

// Scenario: Multiple saves increment version
func TestEvent_MultipleSaves(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	startVersion := cfg.Version

	versions := make([]int, 3)
	for i := 0; i < 3; i++ {
		cfg.Logger.Enabled = !cfg.Logger.Enabled
		v, err := config.Save(ctx, cfg)
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		versions[i] = v
	}

	for i, v := range versions {
		expected := startVersion + i + 1
		if v != expected {
			t.Errorf("save %d: version = %d, want %d", i, v, expected)
		}
	}
}

// Scenario: Config version increases monotonically
func TestEvent_VersionMonotonic(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	lastVersion := cfg.Version

	for i := 0; i < 5; i++ {
		cfg.Logger.Options.LogLevel = i % 4
		v, err := config.Save(ctx, cfg)
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if v <= lastVersion {
			t.Errorf("save %d: version %d <= %d", i, v, lastVersion)
		}
		lastVersion = v
	}
}

// Scenario: Rapid saves don't lose updates
func TestEvent_RapidSaves(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()

	for i := 0; i < 10; i++ {
		cfg.Namespace = fmt.Sprintf("rapid-ns-%d", i)
		if _, err := config.Save(ctx, cfg); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	cfg2 := config.Get()
	if cfg2.Namespace != "rapid-ns-9" {
		t.Errorf("namespace = %q, want %q", cfg2.Namespace, "rapid-ns-9")
	}
}
