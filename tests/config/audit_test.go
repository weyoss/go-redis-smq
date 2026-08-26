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

	"github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/testutil"
)

// Scenario: Enable acknowledged messages audit
func TestAudit_EnableAcknowledged(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be enabled")
	}
}

// Scenario: Enable dead-lettered messages audit
func TestAudit_EnableDeadLettered(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dead-lettered audit should be enabled")
	}
}

// Scenario: Enable unacknowledgment history
func TestAudit_EnableUnacknowledgmentHistory(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.MessageAudit.UnacknowledgementHistory.Enabled {
		t.Error("unacknowledgment history should be enabled")
	}
}

// Scenario: Set queue size limits for audit
func TestAudit_QueueSize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 5000
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.QueueSize = 10000
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.AcknowledgedMessages.QueueSize != 5000 {
		t.Errorf("ack queue size = %d, want 5000", cfg2.MessageAudit.AcknowledgedMessages.QueueSize)
	}
	if cfg2.MessageAudit.DeadLetteredMessages.QueueSize != 10000 {
		t.Errorf("dlq queue size = %d, want 10000", cfg2.MessageAudit.DeadLetteredMessages.QueueSize)
	}
}

// Scenario: Set expire times for audit
func TestAudit_ExpireTimes(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.Expire = 3600
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.Expire = 86400
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.AcknowledgedMessages.Expire != 3600 {
		t.Errorf("ack expire = %d, want 3600", cfg2.MessageAudit.AcknowledgedMessages.Expire)
	}
	if cfg2.MessageAudit.DeadLetteredMessages.Expire != 86400 {
		t.Errorf("dlq expire = %d, want 86400", cfg2.MessageAudit.DeadLetteredMessages.Expire)
	}
}

// Scenario: Set max size for unacknowledgment history
func TestAudit_UnacknowledgmentHistoryMaxSize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 50
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.UnacknowledgementHistory.MaxSize != 50 {
		t.Errorf("max size = %d, want 50", cfg2.MessageAudit.UnacknowledgementHistory.MaxSize)
	}
}

// Scenario: Enable all audit with one setting
func TestAudit_EnableAll(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if !cfg2.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be enabled")
	}
	if !cfg2.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dead-lettered audit should be enabled")
	}
	if !cfg2.MessageAudit.UnacknowledgementHistory.Enabled {
		t.Error("unacknowledgment history should be enabled")
	}
}

// Scenario: Disable all audit
func TestAudit_DisableAll(t *testing.T) {
	ctx := testutil.Setup(t)

	// Enable first
	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	config.Save(ctx, cfg)

	// Disable all
	cfg2 := config.Get()
	cfg2.MessageAudit.AcknowledgedMessages.Enabled = false
	cfg2.MessageAudit.DeadLetteredMessages.Enabled = false
	cfg2.MessageAudit.UnacknowledgementHistory.Enabled = false
	config.Save(ctx, cfg2)

	cfg3 := config.Get()
	if cfg3.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be disabled")
	}
	if cfg3.MessageAudit.DeadLetteredMessages.Enabled {
		t.Error("dead-lettered audit should be disabled")
	}
	if cfg3.MessageAudit.UnacknowledgementHistory.Enabled {
		t.Error("unacknowledgment history should be disabled")
	}
}

// Scenario: Audit defaults are all disabled
func TestAudit_Defaults(t *testing.T) {
	testutil.Setup(t)

	cfg := config.Get()

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
