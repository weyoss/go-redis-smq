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
	"time"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

func TestQueue_TypeStringAndIsValid(t *testing.T) {
	if q.TypeFIFO.String() != "fifo" {
		t.Errorf("TypeFIFO.String() = %q", q.TypeFIFO.String())
	}
	if q.TypeLIFO.String() != "lifo" {
		t.Errorf("TypeLIFO.String() = %q", q.TypeLIFO.String())
	}
	if q.TypePriority.String() != "priority" {
		t.Errorf("TypePriority.String() = %q", q.TypePriority.String())
	}
	if q.QueueType(99).String() != "unknown" {
		t.Errorf("invalid type string = %q", q.QueueType(99).String())
	}

	for _, typ := range []q.QueueType{q.TypeFIFO, q.TypeLIFO, q.TypePriority} {
		if !typ.IsValid() {
			t.Errorf("%v should be valid", typ)
		}
	}
	if q.QueueType(99).IsValid() {
		t.Error("invalid type should not be valid")
	}
}

func TestQueue_DeliveryModelStringAndIsValid(t *testing.T) {
	if q.DeliveryPointToPoint.String() != "point_to_point" {
		t.Errorf("DeliveryPointToPoint.String() = %q", q.DeliveryPointToPoint.String())
	}
	if q.DeliveryPubSub.String() != "pub_sub" {
		t.Errorf("DeliveryPubSub.String() = %q", q.DeliveryPubSub.String())
	}
	if q.DeliveryModel(99).String() != "unknown" {
		t.Errorf("invalid delivery model string = %q", q.DeliveryModel(99).String())
	}
	if !q.DeliveryPointToPoint.IsValid() || !q.DeliveryPubSub.IsValid() {
		t.Error("valid delivery models should be valid")
	}
	if q.DeliveryModel(99).IsValid() {
		t.Error("invalid delivery model should not be valid")
	}
}

func TestQueue_LockOwnerStringAndInt(t *testing.T) {
	if q.LockOwnerPurgeJob.String() != "PURGE_JOB" {
		t.Errorf("LockOwnerPurgeJob.String() = %q", q.LockOwnerPurgeJob.String())
	}
	if q.LockOwnerPurgeJob.Int() != 0 {
		t.Errorf("LockOwnerPurgeJob.Int() = %d", q.LockOwnerPurgeJob.Int())
	}
}

func TestQueue_StateIsValid(t *testing.T) {
	for _, s := range []q.QueueState{q.StateActive, q.StatePaused, q.StateStopped, q.StateLocked} {
		if !s.IsValid() {
			t.Errorf("%v should be valid", s)
		}
	}
	if q.QueueState(99).IsValid() {
		t.Error("invalid state should not be valid")
	}
}

func TestQueue_ParamsClone(t *testing.T) {
	params := q.MustQueueParamsWithNS("orders", "production")
	clone := params.Clone()
	if clone == nil {
		t.Fatal("Clone returned nil")
	}
	if clone == params {
		t.Error("Clone should return a new instance")
	}
	if clone.Name() != params.Name() || clone.NS() != params.NS() {
		t.Errorf("Clone fields mismatch: got %s/%s, want %s/%s", clone.Name(), clone.NS(), params.Name(), params.NS())
	}
}

func TestQueue_RateLimitParamsString(t *testing.T) {
	rl := q.MustRateLimitParams(100, time.Minute)
	if rl.String() == "" {
		t.Error("RateLimitParams.String() returned empty string")
	}
}

func TestQueue_PurgeJobQueueParams(t *testing.T) {
	queueParams := q.MustQueueParams("orders")
	job := &q.PurgeJob{
		Payload: q.PurgeJobPayload{Queue: queueParams},
	}
	if job.QueueParams() != queueParams {
		t.Error("QueueParams returned unexpected value")
	}
}

func TestQueue_CreateWithRateLimit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-create-with-rate-limit")
	rl := q.MustRateLimitParams(10, time.Second)

	if err := queue.NewManager().CreateWithRateLimit(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint, rl); err != nil {
		t.Fatalf("CreateWithRateLimit: %v", err)
	}

	if err := queue.MustExist(ctx, params); err != nil {
		t.Fatalf("MustExist: %v", err)
	}
	if err := queue.MustBeOperational(ctx, params); err != nil {
		t.Fatalf("MustBeOperational: %v", err)
	}
	if err := queue.CanEnqueue(ctx, params); err != nil {
		t.Fatalf("CanEnqueue: %v", err)
	}
	if err := queue.CanDequeue(ctx, params); err != nil {
		t.Fatalf("CanDequeue: %v", err)
	}
}
