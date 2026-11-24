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

// Scenario: Get returns the default config after Init
func TestGet_DefaultConfig(t *testing.T) {
	testutil.Setup(t)

	cfg := config.Get()
	if cfg == nil {
		t.Fatal("config should not be nil")
	}
	if cfg.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", cfg.Namespace, "default")
	}
	if cfg.Logger.Enabled {
		t.Error("logger should be disabled by default")
	}
	if cfg.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be disabled by default")
	}
	if cfg.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dead-lettered audit should be disabled by default")
	}
	if cfg.MessageAudit.UnacknowledgementHistory.Enabled {
		t.Error("unacknowledgment history should be disabled by default")
	}
}

// Scenario: Get returns independent snapshots
//
// Pre-fix, Get returned the shared *Config pointer — two callers held the
// same object, and one mutating it (as the canonical
// "Get, modify, Save" pattern requires) raced every concurrent reader
// (logger, workers, other Get callers). The race detector surfaced this
// under the refactored process-scoped worker layout, where the coordinator
// spawns workers that log at startup during a queue-created event.
//
// Post-fix, each Get returns a private clone. Mutating one snapshot has
// no effect on another, and no race exists.
func TestGet_ReturnsIndependentSnapshots(t *testing.T) {
	testutil.Setup(t)

	cfg1 := config.Get()
	cfg2 := config.Get()

	if cfg1 == cfg2 {
		t.Fatal("Get should return independent snapshots, not the same pointer")
	}

	// Same initial values.
	if *cfg1 != *cfg2 {
		t.Fatal("independent snapshots should have equal values")
	}

	// Mutating one has no effect on the other.
	cfg1.Namespace = "mutated-only-in-cfg1"
	if cfg2.Namespace == "mutated-only-in-cfg1" {
		t.Fatal("mutating cfg1 must not affect cfg2")
	}

	// Mutating one has no effect on the stored snapshot either.
	cfg3 := config.Get()
	if cfg3.Namespace == "mutated-only-in-cfg1" {
		t.Fatal("mutating cfg1 must not affect the stored snapshot")
	}
}

// Scenario: Get returns logger options
func TestGet_LoggerOptions(t *testing.T) {
	testutil.Setup(t)

	cfg := config.Get()
	if !cfg.Logger.Options.IncludeTimestamp {
		t.Error("include timestamp should be true by default")
	}
	if !cfg.Logger.Options.Colorize {
		t.Error("colorize should be true by default")
	}
	if cfg.Logger.Options.LogLevel != 1 { // INFO
		t.Errorf("log level = %d, want 1 (INFO)", cfg.Logger.Options.LogLevel)
	}
}

// Scenario: Get returns message audit defaults
func TestGet_AuditDefaults(t *testing.T) {
	testutil.Setup(t)

	cfg := config.Get()

	if cfg.MessageAudit.AcknowledgedMessages.QueueSize != 0 {
		t.Errorf("ack queue size = %d, want 0", cfg.MessageAudit.AcknowledgedMessages.QueueSize)
	}
	if cfg.MessageAudit.AcknowledgedMessages.Expire != 0 {
		t.Errorf("ack expire = %d, want 0", cfg.MessageAudit.AcknowledgedMessages.Expire)
	}
	if cfg.MessageAudit.DeadLetteredMessages.QueueSize != 0 {
		t.Errorf("dlq queue size = %d, want 0", cfg.MessageAudit.DeadLetteredMessages.QueueSize)
	}
	if cfg.MessageAudit.DeadLetteredMessages.Expire != 0 {
		t.Errorf("dlq expire = %d, want 0", cfg.MessageAudit.DeadLetteredMessages.Expire)
	}
	if cfg.MessageAudit.UnacknowledgementHistory.MaxSize != 100 {
		t.Errorf("unack history max size = %d, want 100", cfg.MessageAudit.UnacknowledgementHistory.MaxSize)
	}
}
