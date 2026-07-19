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

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Pause and resume a queue
func TestQueueState_PauseAndResume(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-pause-resume")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Pause
	_, err := queue.Pause(ctx, params, nil)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != q.StatePaused {
		t.Fatalf("state = %v, want PAUSED", props.OperationalState)
	}

	// Resume
	_, err = queue.Resume(ctx, params, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	props, err = queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != q.StateActive {
		t.Fatalf("state = %v, want ACTIVE", props.OperationalState)
	}
}

// Scenario: Stop and resume a queue
func TestQueueState_StopAndResume(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-stop-resume")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Stop
	_, err := queue.Stop(ctx, params, nil)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	props, err := queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != q.StateStopped {
		t.Fatalf("state = %v, want STOPPED", props.OperationalState)
	}

	// Resume from stopped
	_, err = queue.Resume(ctx, params, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	props, err = queue.Properties(ctx, params)
	if err != nil {
		t.Fatalf("properties: %v", err)
	}
	if props.OperationalState != q.StateActive {
		t.Fatalf("state = %v, want ACTIVE", props.OperationalState)
	}
}

// Scenario: Invalid state transition returns error
func TestQueueState_InvalidTransition(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-invalid-transition")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Stop the queue
	_, err := queue.Stop(ctx, params, nil)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Try to pause a stopped queue (invalid)
	_, err = queue.Pause(ctx, params, nil)
	if err == nil {
		t.Fatal("expected error: cannot pause a stopped queue")
	}
}

// Scenario: State history tracks all transitions
func TestQueueState_History(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-history")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	// Pause
	queue.Pause(ctx, params, nil)
	// Resume
	queue.Resume(ctx, params, nil)
	// Stop
	queue.Stop(ctx, params, nil)

	history, err := queue.History(ctx, params)
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	// Should have at least 3 transitions: initial ACTIVE → PAUSED → ACTIVE → STOPPED
	if len(history) < 3 {
		t.Fatalf("history length = %d, want >= 3", len(history))
	}

	// Most recent first: STOPPED
	if history[0].To != q.StateStopped {
		t.Errorf("most recent state = %v, want STOPPED", history[0].To)
	}
}

// Scenario: Current state reflects latest transition
func TestQueueState_Current(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-current")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	transition, err := queue.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != q.StateActive {
		t.Fatalf("state = %v, want ACTIVE", transition.To)
	}

	queue.Pause(ctx, params, nil)

	transition, err = queue.Current(ctx, params)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if transition.To != q.StatePaused {
		t.Fatalf("state = %v, want PAUSED", transition.To)
	}
}
