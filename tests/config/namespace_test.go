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

	redissmq "github.com/weyoss/go-redis-smq"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/config"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Default namespace is used when no namespace specified
func TestNamespace_DefaultUsed(t *testing.T) {
	ctx := testutil.Setup(t)

	// Queue created without namespace uses default
	params := publicqueue.MustQueueParams("test-ns-default")
	if err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Should be in default namespace
	if params.NS() != "default" {
		t.Errorf("ns = %q, want %q", params.NS(), "default")
	}
}

// Scenario: Change namespace via config
func TestNamespace_ChangeViaConfig(t *testing.T) {
	ctx := testutil.Setup(t)

	// Change default namespace
	cfg := config.Get()
	cfg.Namespace = "production"
	if _, err := config.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	// New queue without namespace uses new default
	params := publicqueue.MustQueueParams("test-ns-changed")
	if err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint); err != nil {
		t.Fatalf("create: %v", err)
	}

	if params.NS() != "production" {
		t.Errorf("ns = %q, want %q", params.NS(), "production")
	}
}

// Scenario: Explicit namespace overrides default
func TestNamespace_ExplicitOverridesDefault(t *testing.T) {
	ctx := testutil.Setup(t)

	// Change default
	cfg := config.Get()
	cfg.Namespace = "production"
	config.Save(ctx, cfg)

	// Create queue with explicit namespace
	params := publicqueue.MustQueueParamsWithNS("test-ns-explicit", "staging")
	if err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Should use explicit namespace, not default
	if params.NS() != "staging" {
		t.Errorf("ns = %q, want %q", params.NS(), "staging")
	}
}

// Scenario: Namespace survives config reload
func TestNamespace_SurvivesReload(t *testing.T) {
	ctx := testutil.Setup(t)

	// Set namespace
	cfg := config.Get()
	cfg.Namespace = "persistent-ns"
	config.Save(ctx, cfg)

	// Get again — should have the saved namespace
	cfg2 := config.Get()
	if cfg2.Namespace != "persistent-ns" {
		t.Errorf("namespace = %q, want %q", cfg2.Namespace, "persistent-ns")
	}
}

// Scenario: Queue discovery by namespace
func TestNamespace_DiscoveryByNamespace(t *testing.T) {
	ctx := testutil.Setup(t)

	// Set namespace and create queue
	cfg := config.Get()
	cfg.Namespace = "discovery-ns"
	config.Save(ctx, cfg)

	params := publicqueue.MustQueueParams("test-ns-discovery")
	internalQueue.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	// Should find queue in the namespace
	queues, err := redissmq.NewQueueManager().ListByNamespace(ctx, "discovery-ns")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := false
	for _, qp := range queues {
		if qp.String() == params.String() {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("queue %s not found in namespace discovery-ns", params.String())
	}
}
