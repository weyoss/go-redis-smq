/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package namespace_test

import (
	"fmt"
	"strings"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Very long namespace name
func TestEdge_VeryLongName(t *testing.T) {
	ctx := testutil.Setup(t)

	longNS := strings.Repeat("a", 200)
	params := publicqueue.MustQueueParamsWithNS("test-edge-long-q", longNS)
	err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create with long namespace: %v", err)
	}

	nm := redissmq.NewNamespaceManager()
	exists, _ := nm.Exists(ctx, longNS)
	if !exists {
		t.Error("namespace with long name should exist")
	}
}

// Scenario: Namespace with dots, hyphens, underscores
func TestEdge_SpecialCharacters(t *testing.T) {
	ctx := testutil.Setup(t)

	specialNS := "my-ns.with_special-chars.and.dots"
	params := publicqueue.MustQueueParamsWithNS("test-edge-special-q", specialNS)
	err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create with special chars namespace: %v", err)
	}

	nm := redissmq.NewNamespaceManager()
	exists, _ := nm.Exists(ctx, specialNS)
	if !exists {
		t.Error("namespace with special chars should exist")
	}
}

// Scenario: Rapid create and delete cycles
func TestEdge_RapidCreateDelete(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := redissmq.NewNamespaceManager()

	qm := redissmq.NewQueueManager()
	for i := 0; i < 10; i++ {
		nsName := fmt.Sprintf("rapid-ns-%d", i)
		params := publicqueue.MustQueueParamsWithNS("rapid-q", nsName)
		qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
		nm.Delete(ctx, nsName)

		exists, _ := nm.Exists(ctx, nsName)
		if exists {
			t.Errorf("namespace %s should be deleted", nsName)
		}
	}
}

// Scenario: Namespace name at minimum length
func TestEdge_MinimumNameLength(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParamsWithNS("test-edge-min-q", "a")
	err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create with single char namespace: %v", err)
	}

	nm := redissmq.NewNamespaceManager()
	exists, _ := nm.Exists(ctx, "a")
	if !exists {
		t.Error("single char namespace should exist")
	}
}

// Scenario: Multiple queues across many namespaces
func TestEdge_ManyNamespaces(t *testing.T) {
	ctx := testutil.Setup(t)

	nm := redissmq.NewNamespaceManager()
	count := 20

	qm := redissmq.NewQueueManager()
	for i := 0; i < count; i++ {
		nsName := fmt.Sprintf("many-ns-%d", i)
		params := publicqueue.MustQueueParamsWithNS("many-q", nsName)
		qm.Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	}

	namespaces, err := nm.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	found := 0
	for _, ns := range namespaces {
		if strings.HasPrefix(ns, "many-ns-") {
			found++
		}
	}
	if found < count {
		t.Errorf("found %d namespaces, want >= %d", found, count)
	}
}

// Scenario: Namespace name with only valid characters
func TestEdge_AllValidCharacters(t *testing.T) {
	ctx := testutil.Setup(t)

	validNS := "a-b.c_d-e.f-g.h"
	params := publicqueue.MustQueueParamsWithNS("test-edge-valid-q", validNS)
	err := redissmq.NewQueueManager().Create(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)
	if err != nil {
		t.Fatalf("create with valid chars namespace: %v", err)
	}

	nm := redissmq.NewNamespaceManager()
	exists, _ := nm.Exists(ctx, validNS)
	if !exists {
		t.Error("namespace with all valid chars should exist")
	}
}
