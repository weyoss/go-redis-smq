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

// Scenario: Save config updates version
func TestSave_UpdatesVersion(t *testing.T) {
	ctx := testutil.Setup(t)

	c := config.Get()
	oldVersion := c.Version

	newVersion, err := config.Save(ctx, c)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if newVersion <= oldVersion {
		t.Errorf("version = %d, want > %d", newVersion, oldVersion)
	}

	// In-memory config should have updated version
	if c.Version != newVersion {
		t.Errorf("in-memory version = %d, want %d", c.Version, newVersion)
	}
}

// Scenario: Save persists to Redis and survives Get
func TestSave_Persists(t *testing.T) {
	ctx := testutil.Setup(t)

	c := config.Get()
	c.Namespace = "test-persist-ns"

	_, err := config.Save(ctx, c)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	// Get should return persisted value
	c2 := config.Get()
	if c2.Namespace != "test-persist-ns" {
		t.Errorf("namespace = %q, want %q", c2.Namespace, "test-persist-ns")
	}
}

// Scenario: Save with partial update via get → modify → save
func TestSave_PartialUpdate(t *testing.T) {
	ctx := testutil.Setup(t)

	// Get current config, modify, save
	c := config.Get()
	c.Logger.Enabled = true
	c.Logger.Options.LogLevel = 0 // DEBUG

	_, err := config.Save(ctx, c)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify logger is enabled
	c2 := config.Get()
	if !c2.Logger.Enabled {
		t.Error("logger should be enabled")
	}
	if c2.Logger.Options.LogLevel != 0 {
		t.Errorf("log level = %d, want 0 (DEBUG)", c2.Logger.Options.LogLevel)
	}

	// Other settings should be unchanged
	if c2.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", c2.Namespace, "default")
	}
}

// Scenario: Save audit settings
func TestSave_AuditSettings(t *testing.T) {
	ctx := testutil.Setup(t)

	c := config.Get()
	c.MessageAudit.AcknowledgedMessages.Enabled = true
	c.MessageAudit.AcknowledgedMessages.QueueSize = 5000
	c.MessageAudit.AcknowledgedMessages.Expire = 3600

	_, err := config.Save(ctx, c)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	c2 := config.Get()
	if !c2.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be enabled")
	}
	if c2.MessageAudit.AcknowledgedMessages.QueueSize != 5000 {
		t.Errorf("queue size = %d, want 5000", c2.MessageAudit.AcknowledgedMessages.QueueSize)
	}
	if c2.MessageAudit.AcknowledgedMessages.Expire != 3600 {
		t.Errorf("expire = %d, want 3600", c2.MessageAudit.AcknowledgedMessages.Expire)
	}
}

// Scenario: Multiple saves increment version
func TestSave_MultipleSaves(t *testing.T) {
	ctx := testutil.Setup(t)

	c := config.Get()
	initialVersion := c.Version

	for i := 1; i <= 3; i++ {
		c.Logger.Enabled = !c.Logger.Enabled
		version, err := config.Save(ctx, c)
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if version != initialVersion+i {
			t.Errorf("save %d: version = %d, want %d", i, version, initialVersion+i)
		}
	}
}
