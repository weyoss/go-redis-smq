/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message_test

import (
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/pkg/message"
)

func TestMessage_SetDefaultConsumeOptions(t *testing.T) {
	// Save current defaults to restore after test.
	original := message.DefaultConsumeOptions()
	defer message.SetDefaultConsumeOptions(original)

	// Set custom defaults.
	message.SetDefaultConsumeOptions(message.ConsumeOptions{
		TTL:            1234,
		RetryThreshold: 5,
		RetryDelay:     6789,
		ConsumeTimeout: 9876,
	})

	// New message should inherit these defaults.
	m := message.New()

	if got := m.TTL(); got != time.Duration(1234)*time.Millisecond {
		t.Errorf("TTL = %v, want 1234ms", got)
	}
	if got := m.RetryThreshold(); got != 5 {
		t.Errorf("RetryThreshold = %d, want 5", got)
	}
	if got := m.RetryDelay(); got != time.Duration(6789)*time.Millisecond {
		t.Errorf("RetryDelay = %v, want 6789ms", got)
	}
	if got := m.ConsumeTimeout(); got != time.Duration(9876)*time.Millisecond {
		t.Errorf("ConsumeTimeout = %v, want 9876ms", got)
	}
}

func TestMessage_SetDefaultConsumeOptions_IgnoresNegative(t *testing.T) {
	original := message.DefaultConsumeOptions()
	defer message.SetDefaultConsumeOptions(original)

	// Attempt to set negative values – should be ignored.
	message.SetDefaultConsumeOptions(message.ConsumeOptions{
		TTL:            -1,
		RetryThreshold: -1,
		RetryDelay:     -1,
		ConsumeTimeout: -1,
	})

	// Defaults should remain unchanged.
	after := message.DefaultConsumeOptions()
	if after.TTL != original.TTL {
		t.Errorf("TTL default changed: got %d, want %d", after.TTL, original.TTL)
	}
	if after.RetryThreshold != original.RetryThreshold {
		t.Errorf("RetryThreshold default changed: got %d, want %d", after.RetryThreshold, original.RetryThreshold)
	}
	if after.RetryDelay != original.RetryDelay {
		t.Errorf("RetryDelay default changed: got %d, want %d", after.RetryDelay, original.RetryDelay)
	}
	if after.ConsumeTimeout != original.ConsumeTimeout {
		t.Errorf("ConsumeTimeout default changed: got %d, want %d", after.ConsumeTimeout, original.ConsumeTimeout)
	}
}

func TestMessage_CreatedAt(t *testing.T) {
	before := time.Now()
	m := message.New()
	after := time.Now()

	created := m.CreatedAt()
	if created.Before(before) || created.After(after) {
		t.Errorf("CreatedAt = %v, want between %v and %v", created, before, after)
	}
	if created.IsZero() {
		t.Fatal("CreatedAt should not be zero")
	}
}
