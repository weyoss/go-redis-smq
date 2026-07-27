/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events_test

import (
	"sync"
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	configEvents "github.com/weyoss/go-redis-smq/pkg/config/events"
)

// Scenario: Subscribe to config updated event
func TestConfigEvents_SubscribeUpdated(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.UpdatedPayload
	sub, err := configEvents.SubscribeUpdated(func(p events.UpdatedPayload) {
		received = p
		wg.Done()
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	cfg := config.Get()
	cfg.Logger.Enabled = true
	version, err := config.Save(ctx, cfg)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Version != version {
		t.Errorf("version = %d, want %d", received.Version, version)
	}
	if received.Config == nil {
		t.Fatal("config should not be nil")
	}
	if !received.Config.Logger.Enabled {
		t.Error("logger should be enabled")
	}
}

// Scenario: Config updated event includes full config
func TestConfigEvents_UpdatedIncludesFullConfig(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(1)

	var received events.UpdatedPayload
	sub, _ := configEvents.SubscribeUpdated(func(p events.UpdatedPayload) {
		received = p
		wg.Done()
	})
	defer sub.Unsubscribe()

	cfg := config.Get()
	cfg.Namespace = "event-test-ns"
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.AcknowledgedMessages.QueueSize = 5000
	config.Save(ctx, cfg)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	if received.Config.Namespace != "event-test-ns" {
		t.Errorf("namespace = %s, want event-test-ns", received.Config.Namespace)
	}
	if !received.Config.MessageAudit.AcknowledgedMessages.Enabled {
		t.Error("acknowledged audit should be enabled")
	}
	if received.Config.MessageAudit.AcknowledgedMessages.QueueSize != 5000 {
		t.Errorf("queue size = %d, want 5000", received.Config.MessageAudit.AcknowledgedMessages.QueueSize)
	}
}

// Scenario: Config updated event version is monotonically increasing
func TestConfigEvents_VersionMonotonic(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var versions []int

	sub, _ := configEvents.SubscribeUpdated(func(p events.UpdatedPayload) {
		mu.Lock()
		versions = append(versions, p.Version)
		mu.Unlock()
	})
	defer sub.Unsubscribe()

	cfg := config.Get()
	for i := 0; i < 3; i++ {
		cfg.Logger.Enabled = !cfg.Logger.Enabled
		config.Save(ctx, cfg)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(versions) != 3 {
		t.Fatalf("expected 3 events, got %d", len(versions))
	}

	for i := 1; i < len(versions); i++ {
		if versions[i] <= versions[i-1] {
			t.Errorf("version not monotonic: %d <= %d", versions[i], versions[i-1])
		}
	}
}

// Scenario: Multiple subscribers for config updated event
func TestConfigEvents_MultipleSubscribers(t *testing.T) {
	ctx := testutil.Setup(t)

	var wg sync.WaitGroup
	wg.Add(3)

	var mu sync.Mutex
	var received []events.UpdatedPayload

	handler := func(p events.UpdatedPayload) {
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		wg.Done()
	}

	sub1, _ := configEvents.SubscribeUpdated(handler)
	sub2, _ := configEvents.SubscribeUpdated(handler)
	sub3, _ := configEvents.SubscribeUpdated(handler)
	defer sub1.Unsubscribe()
	defer sub2.Unsubscribe()
	defer sub3.Unsubscribe()

	cfg := config.Get()
	cfg.Logger.Enabled = true
	config.Save(ctx, cfg)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for events")
	}

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 3 {
		t.Fatalf("expected 3 events, got %d", count)
	}
}

// Scenario: Unsubscribe stops receiving config events
func TestConfigEvents_Unsubscribe(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var eventCount int

	sub, _ := configEvents.SubscribeUpdated(func(p events.UpdatedPayload) {
		mu.Lock()
		eventCount++
		mu.Unlock()
	})

	cfg := config.Get()

	// First save — should trigger event
	cfg.Logger.Enabled = true
	config.Save(ctx, cfg)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	firstCount := eventCount
	mu.Unlock()

	if firstCount != 1 {
		t.Fatalf("expected 1 event before unsubscribe, got %d", firstCount)
	}

	// Unsubscribe
	sub.Unsubscribe()

	// Second save — should NOT trigger event
	cfg.Logger.Enabled = false
	config.Save(ctx, cfg)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	secondCount := eventCount
	mu.Unlock()

	if secondCount != 1 {
		t.Fatalf("expected still 1 event after unsubscribe, got %d", secondCount)
	}
}

// Scenario: Config updated event fires for all config changes
func TestConfigEvents_AllConfigChanges(t *testing.T) {
	ctx := testutil.Setup(t)

	var mu sync.Mutex
	var eventCount int

	sub, _ := configEvents.SubscribeUpdated(func(p events.UpdatedPayload) {
		mu.Lock()
		eventCount++
		mu.Unlock()
	})
	defer sub.Unsubscribe()

	cfg := config.Get()

	// Change namespace
	cfg.Namespace = "events-test-ns"
	config.Save(ctx, cfg)
	time.Sleep(100 * time.Millisecond)

	// Change logger
	cfg.Logger.Enabled = true
	cfg.Logger.Options.LogLevel = 0
	config.Save(ctx, cfg)
	time.Sleep(100 * time.Millisecond)

	// Change audit settings
	cfg.MessageAudit.AcknowledgedMessages.Enabled = true
	cfg.MessageAudit.DeadLetteredMessages.Enabled = true
	config.Save(ctx, cfg)
	time.Sleep(100 * time.Millisecond)

	// Change unacknowledgment history
	cfg.MessageAudit.UnacknowledgementHistory.Enabled = true
	cfg.MessageAudit.UnacknowledgementHistory.MaxSize = 50
	config.Save(ctx, cfg)
	time.Sleep(100 * time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	count := eventCount
	mu.Unlock()

	if count != 4 {
		t.Fatalf("expected 4 events, got %d", count)
	}
}
