/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// waitForCondition polls a condition until it returns true or times out.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestQueueStateTracker_IsPaused(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-state-tracker-paused")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	tracker := internalconsumer.NewQueueStateTracker(nil, nil, nil, nil)
	defer tracker.Shutdown()

	if tracker.IsPaused(params) {
		t.Fatal("queue should not be paused initially")
	}

	sm := redissmq.NewStateManager()
	if _, err := sm.Pause(ctx, params, nil); err != nil {
		t.Fatalf("pause: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return tracker.IsPaused(params)
	})

	if tracker.IsStopped(params) || tracker.IsLocked(params) {
		t.Error("queue should only be paused, not stopped or locked")
	}

	// Resume and verify paused flag clears.
	if _, err := sm.Resume(ctx, params, nil); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return !tracker.IsPaused(params)
	})
}

func TestQueueStateTracker_IsStopped(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-state-tracker-stopped")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	tracker := internalconsumer.NewQueueStateTracker(nil, nil, nil, nil)
	defer tracker.Shutdown()

	if tracker.IsStopped(params) {
		t.Fatal("queue should not be stopped initially")
	}

	sm := redissmq.NewStateManager()
	if _, err := sm.Stop(ctx, params, nil); err != nil {
		t.Fatalf("stop: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return tracker.IsStopped(params)
	})

	if tracker.IsPaused(params) || tracker.IsLocked(params) {
		t.Error("queue should only be stopped, not paused or locked")
	}

	// Resume and verify stopped flag clears.
	if _, err := sm.Resume(ctx, params, nil); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return !tracker.IsStopped(params)
	})
}

func TestQueueStateTracker_IsLocked(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-state-tracker-locked")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	tracker := internalconsumer.NewQueueStateTracker(nil, nil, nil, nil)
	defer tracker.Shutdown()

	if tracker.IsLocked(params) {
		t.Fatal("queue should not be locked initially")
	}

	sm := redissmq.NewStateManager()
	lockID := "lock-123"
	if _, err := sm.Lock(ctx, params, queue.LockOwnerPurgeJob, lockID, nil); err != nil {
		t.Fatalf("lock: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		return tracker.IsLocked(params)
	})

	if tracker.IsPaused(params) || tracker.IsStopped(params) {
		t.Error("queue should only be locked, not paused or stopped")
	}

	// Unlock and verify locked flag clears.
	if _, err := sm.Unlock(ctx, params, queue.LockOwnerPurgeJob, lockID, nil); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return !tracker.IsLocked(params)
	})
}
