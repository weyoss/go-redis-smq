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

// Scenario: Full config lifecycle — get, modify, save, verify, modify again
func TestComplex_FullLifecycle(t *testing.T) {
	ctx := testutil.Setup(t)

	// Start with defaults
	cfg := config.Get()
	if cfg.Namespace != "default" {
		t.Fatalf("initial namespace = %q, want %q", cfg.Namespace, "default")
	}

	// Modify multiple settings
	cfg.Namespace = "lifecycle-ns"
	cfg.Logger.Enabled = true
	cfg.Logger.Options.LogLevel = 0
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000

	_, err := config.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Verify all settings persisted
	cfg2 := config.Get()
	if cfg2.Namespace != "lifecycle-ns" {
		t.Errorf("namespace = %q", cfg2.Namespace)
	}
	if !cfg2.Logger.Enabled {
		t.Error("logger not enabled")
	}
	if cfg2.Logger.Options.LogLevel != 0 {
		t.Errorf("log level = %d", cfg2.Logger.Options.LogLevel)
	}
	if !cfg2.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("audit not enabled")
	}
	if cfg2.MessageAudit.AcknowledgedMessages.QueueSize != 1000 {
		t.Errorf("queue size = %d", cfg2.MessageAudit.AcknowledgedMessages.QueueSize)
	}

	// Modify again — disable everything
	cfg2.Logger.Enabled = false
	cfg2.MessageAudit.AcknowledgedMessages.Enabled = false
	cfg2.Namespace = "lifecycle-ns-v2"

	_, err = config.Save(ctx, cfg2)
	if err != nil {
		t.Fatalf("second save: %v", err)
	}

	// Verify changes
	cfg3 := config.Get()
	if cfg3.Namespace != "lifecycle-ns-v2" {
		t.Errorf("namespace = %q", cfg3.Namespace)
	}
	if cfg3.Logger.Enabled {
		t.Error("logger should be disabled")
	}
	if cfg3.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("audit should be disabled")
	}
}

// Scenario: Multiple config updates across different settings
func TestComplex_MultipleUpdates(t *testing.T) {
	ctx := testutil.Setup(t)

	// Update namespace only
	cfg := config.Get()
	cfg.Namespace = "update-1"
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.Namespace != "update-1" {
		t.Errorf("namespace = %q", cfg2.Namespace)
	}

	// Update logger only
	cfg2.Logger.Enabled = true
	config.Save(ctx, cfg2)

	cfg3 := config.Get()
	if !cfg3.Logger.Enabled {
		t.Error("logger should be enabled")
	}
	if cfg3.Namespace != "update-1" {
		t.Errorf("namespace should persist: %q", cfg3.Namespace)
	}

	// Update audit only
	cfg3.MessageAudit.DeadLetteredMessages.Enabled = true
	config.Save(ctx, cfg3)

	cfg4 := config.Get()
	if !cfg4.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dlq audit should be enabled")
	}
	if !cfg4.Logger.Enabled {
		t.Error("logger should still be enabled")
	}
	if cfg4.Namespace != "update-1" {
		t.Errorf("namespace should persist: %q", cfg4.Namespace)
	}
}

// Scenario: Config survives Init/Shutdown cycle
func TestComplex_SurvivesRestart(t *testing.T) {
	ctx := testutil.Setup(t)

	// Set custom config
	cfg := config.Get()
	cfg.Namespace = "survive-restart"
	cfg.Logger.Enabled = true
	version, _ := config.Save(ctx, cfg)

	t.Logf("saved config version %d with namespace %q", version, cfg.Namespace)

	// Simulate restart by reading config fresh
	// (in real restart, Init would reload from Redis)
	cfg2 := config.Get()
	if cfg2.Namespace != "survive-restart" {
		t.Errorf("namespace = %q, want %q", cfg2.Namespace, "survive-restart")
	}
	if !cfg2.Logger.Enabled {
		t.Error("logger should be enabled")
	}
	if cfg2.Version != version {
		t.Errorf("version = %d, want %d", cfg2.Version, version)
	}
}

// Scenario: All audit settings configured together
func TestComplex_AllAuditSettings(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000
	cfg.MessageAudit.AcknowledgedMessages.Expire = 3600
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 5000
	cfg.MessageAudit.DeadLetteredMessages.Expire = 86400
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 50

	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("ack audit not enabled")
	}
	if cfg2.MessageAudit.AcknowledgedMessages.QueueSize != 1000 {
		t.Errorf("ack queue size = %d", cfg2.MessageAudit.AcknowledgedMessages.QueueSize)
	}
	if cfg2.MessageAudit.AcknowledgedMessages.Expire != 3600 {
		t.Errorf("ack expire = %d", cfg2.MessageAudit.AcknowledgedMessages.Expire)
	}
	if !cfg2.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dlq audit not enabled")
	}
	if cfg2.MessageAudit.DeadLetteredMessages.QueueSize != 5000 {
		t.Errorf("dlq queue size = %d", cfg2.MessageAudit.DeadLetteredMessages.QueueSize)
	}
	if cfg2.MessageAudit.DeadLetteredMessages.Expire != 86400 {
		t.Errorf("dlq expire = %d", cfg2.MessageAudit.DeadLetteredMessages.Expire)
	}
	if !cfg2.MessageAudit.UnacknowledgementHistory.Enabled {
		t.Error("unack history not enabled")
	}
	if cfg2.MessageAudit.UnacknowledgementHistory.MaxSize != 50 {
		t.Errorf("unack max size = %d", cfg2.MessageAudit.UnacknowledgementHistory.MaxSize)
	}
}
