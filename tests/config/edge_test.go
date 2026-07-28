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
	"strings"
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
)

// Scenario: Very long namespace
func TestEdge_VeryLongNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	longNS := strings.Repeat("a", 200)
	cfg := config.Get()
	cfg.Namespace = longNS
	_, err := config.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("save with long namespace: %v", err)
	}

	cfg2 := config.Get()
	if cfg2.Namespace != longNS {
		t.Errorf("namespace length = %d, want %d", len(cfg2.Namespace), len(longNS))
	}
}

// Scenario: Special characters in namespace
func TestEdge_SpecialCharsNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	specialNS := "test-ns.with_special-chars.and.dots"
	cfg := config.Get()
	cfg.Namespace = specialNS
	_, err := config.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("save with special chars: %v", err)
	}

	cfg2 := config.Get()
	if cfg2.Namespace != specialNS {
		t.Errorf("namespace = %q, want %q", cfg2.Namespace, specialNS)
	}
}

// Scenario: Empty namespace
func TestEdge_EmptyNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.Namespace = ""
	_, err := config.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("save with empty namespace: %v", err)
	}

	cfg2 := config.Get()
	t.Logf("namespace after empty save: %q", cfg2.Namespace)
}

// Scenario: Large audit queue size
func TestEdge_LargeAuditQueueSize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 1000000
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.AcknowledgedMessages.QueueSize != 1000000 {
		t.Errorf("queue size = %d", cfg2.MessageAudit.AcknowledgedMessages.QueueSize)
	}
}

// Scenario: Zero expire time
func TestEdge_ZeroExpireTime(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.Expire = 0
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.AcknowledgedMessages.Expire != 0 {
		t.Errorf("expire = %d, want 0", cfg2.MessageAudit.AcknowledgedMessages.Expire)
	}
}

// Scenario: Very long expire time
func TestEdge_VeryLongExpire(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.Expire = 31536000
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.DeadLetteredMessages.Expire != 31536000 {
		t.Errorf("expire = %d", cfg2.MessageAudit.DeadLetteredMessages.Expire)
	}
}

// Scenario: Maximum unacknowledgment history size
func TestEdge_MaxUnackHistorySize(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 0
	config.Save(ctx, cfg)

	cfg2 := config.Get()
	if cfg2.MessageAudit.UnacknowledgementHistory.MaxSize != 0 {
		t.Errorf("max size = %d, want 0", cfg2.MessageAudit.UnacknowledgementHistory.MaxSize)
	}
}

// Scenario: Rapid toggle of settings
func TestEdge_RapidToggle(t *testing.T) {
	ctx := testutil.Setup(t)

	cfg := config.Get()

	for i := 0; i < 20; i++ {
		cfg.Logger.Enabled = i%2 == 0
		if _, err := config.Save(ctx, cfg); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	cfg2 := config.Get()
	expected := false
	if cfg2.Logger.Enabled != expected {
		t.Errorf("logger enabled = %v, want %v", cfg2.Logger.Enabled, expected)
	}
}
