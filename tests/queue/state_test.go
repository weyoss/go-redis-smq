/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue_test

import (
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Pause and resume a queue
func TestQueueState_PauseAndResume(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-pause-resume")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()

	// Pause
	_, err := sm.Pause(ctx, params, nil)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}

	qm := redissmq.NewQueueManager()

	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != publicqueue.StatePaused {
		t.Fatalf("state = %v, want PAUSED", props.OperationalState)
	}

	// Resume
	_, err = sm.Resume(ctx, params, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	props, err = qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != publicqueue.StateActive {
		t.Fatalf("state = %v, want ACTIVE", props.OperationalState)
	}
}

// Scenario: Stop and resume a queue
func TestQueueState_StopAndResume(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-stop-resume")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()

	// Stop
	_, err := sm.Stop(ctx, params, nil)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	qm := redissmq.NewQueueManager()

	props, err := qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != publicqueue.StateStopped {
		t.Fatalf("state = %v, want STOPPED", props.OperationalState)
	}

	// Resume from stopped
	_, err = sm.Resume(ctx, params, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	props, err = qm.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != publicqueue.StateActive {
		t.Fatalf("state = %v, want ACTIVE", props.OperationalState)
	}
}

// Scenario: Invalid state transition returns error
func TestQueueState_InvalidTransition(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-invalid-transition")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()

	// Stop the queue
	_, err := sm.Stop(ctx, params, nil)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Try to pause a stopped queue (invalid)
	_, err = sm.Pause(ctx, params, nil)
	if err == nil {
		t.Fatal("expected error: cannot pause a stopped queue")
	}
}

// Scenario: State history tracks all transitions
func TestQueueState_History(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-history")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()

	// Pause
	sm.Pause(ctx, params, nil)
	// Resume
	sm.Resume(ctx, params, nil)
	// Stop
	sm.Stop(ctx, params, nil)

	history, err := sm.History(ctx, params)
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	// Should have at least 3 transitions: initial ACTIVE → PAUSED → ACTIVE → STOPPED
	if len(history) < 3 {
		t.Fatalf("history length = %d, want >= 3", len(history))
	}

	// Most recent first: STOPPED
	if history[0].To != publicqueue.StateStopped {
		t.Errorf("most recent state = %v, want STOPPED", history[0].To)
	}
}

// Scenario: Current state reflects latest transition
func TestQueueState_Current(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-current")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()

	transition, err := sm.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != publicqueue.StateActive {
		t.Fatalf("state = %v, want ACTIVE", transition.To)
	}

	sm.Pause(ctx, params, nil)

	transition, err = sm.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != publicqueue.StatePaused {
		t.Fatalf("state = %v, want PAUSED", transition.To)
	}
}
