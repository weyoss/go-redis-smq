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
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
)

// Scenario: Enable and disable logger
func TestLogger_EnableDisable(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()

	// Enable
	cfg.Logger.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.Logger.Enabled {
		t.Error("logger should be enabled")
	}

	// Disable
	cfg2.Logger.Enabled = false
	config.Save(ctx, cfg2)

	cfg3 := config.Get()
	if cfg3.Logger.Enabled {
		t.Error("logger should be disabled")
	}
}

// Scenario: Change log level
func TestLogger_LogLevel(t *testing.T) {
	ctx := testutil.Setup(t)

	levels := []struct {
		level int
		name  string
	}{
		{0, "DEBUG"},
		{1, "INFO"},
		{2, "WARN"},
		{3, "ERROR"},
	}

	for _, l := range levels {
		cfg := config.Get()
		cfg.Logger.Enabled = true
		cfg.Logger.Options.LogLevel = l.level
		config.Save(ctx, cfg)

		cfg2 := config.Get()
		if cfg2.Logger.Options.LogLevel != l.level {
			t.Errorf("log level = %d, want %d (%s)", cfg2.Logger.Options.LogLevel, l.level, l.name)
		}
	}
}

// Scenario: Logger with timestamps
func TestLogger_Timestamps(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.Logger.Enabled = true
	cfg.Logger.Options.IncludeTimestamp = false
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.Logger.Options.IncludeTimestamp {
		t.Error("timestamps should be disabled")
	}

	cfg2.Logger.Options.IncludeTimestamp = true
	config.Save(ctx, cfg2)

	cfg3 := config.Get()
	if !cfg3.Logger.Options.IncludeTimestamp {
		t.Error("timestamps should be enabled")
	}
}

// Scenario: Logger with colors
func TestLogger_Colors(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.Logger.Enabled = true
	cfg.Logger.Options.Colorize = false
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.Logger.Options.Colorize {
		t.Error("colors should be disabled")
	}

	cfg2.Logger.Options.Colorize = true
	config.Save(ctx, cfg2)

	cfg3 := config.Get()
	if !cfg3.Logger.Options.Colorize {
		t.Error("colors should be enabled")
	}
}

// Scenario: Default logger settings
func TestLogger_Defaults(t *testing.T) {
	testutil.Setup(t)

	cfg := config.Get()

	if cfg.Logger.Enabled {
		t.Error("logger should be disabled by default")
	}
	if !cfg.Logger.Options.IncludeTimestamp {
		t.Error("include timestamp should be true by default")
	}
	if !cfg.Logger.Options.Colorize {
		t.Error("colorize should be true by default")
	}
	if cfg.Logger.Options.LogLevel != 1 {
		t.Errorf("log level = %d, want 1 (INFO)", cfg.Logger.Options.LogLevel)
	}
}
