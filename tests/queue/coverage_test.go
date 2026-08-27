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

	"github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

func TestQueue_TypeStringAndIsValid(t *testing.T) {
	if publicqueue.TypeFIFO.String() != "fifo" {
		t.Errorf("TypeFIFO.String() = %q", publicqueue.TypeFIFO.String())
	}
	if publicqueue.TypeLIFO.String() != "lifo" {
		t.Errorf("TypeLIFO.String() = %q", publicqueue.TypeLIFO.String())
	}
	if publicqueue.TypePriority.String() != "priority" {
		t.Errorf("TypePriority.String() = %q", publicqueue.TypePriority.String())
	}
	if publicqueue.Type(99).String() != "unknown" {
		t.Errorf("invalid type string = %q", publicqueue.Type(99).String())
	}

	for _, typ := range []publicqueue.Type{publicqueue.TypeFIFO, publicqueue.TypeLIFO, publicqueue.TypePriority} {
		if !typ.IsValid() {
			t.Errorf("%v should be valid", typ)
		}
	}
	if publicqueue.Type(99).IsValid() {
		t.Error("invalid type should not be valid")
	}
}

func TestQueue_DeliveryModelStringAndIsValid(t *testing.T) {
	if publicqueue.DeliveryPointToPoint.String() != "point_to_point" {
		t.Errorf("DeliveryPointToPoint.String() = %q", publicqueue.DeliveryPointToPoint.String())
	}
	if publicqueue.DeliveryPubSub.String() != "pub_sub" {
		t.Errorf("DeliveryPubSub.String() = %q", publicqueue.DeliveryPubSub.String())
	}
	if publicqueue.DeliveryModel(99).String() != "unknown" {
		t.Errorf("invalid delivery model string = %q", publicqueue.DeliveryModel(99).String())
	}
	if !publicqueue.DeliveryPointToPoint.IsValid() || !publicqueue.DeliveryPubSub.IsValid() {
		t.Error("valid delivery models should be valid")
	}
	if publicqueue.DeliveryModel(99).IsValid() {
		t.Error("invalid delivery model should not be valid")
	}
}

func TestQueue_LockOwnerStringAndInt(t *testing.T) {
	if publicqueue.LockOwnerPurgeJob.String() != "PURGE_JOB" {
		t.Errorf("LockOwnerPurgeJob.String() = %q", publicqueue.LockOwnerPurgeJob.String())
	}
	if publicqueue.LockOwnerPurgeJob.Int() != 0 {
		t.Errorf("LockOwnerPurgeJob.Int() = %d", publicqueue.LockOwnerPurgeJob.Int())
	}
}

func TestQueue_StateIsValid(t *testing.T) {
	for _, s := range []publicqueue.QueueState{
		publicqueue.StateActive, publicqueue.StatePaused,
		publicqueue.StateStopped, publicqueue.StateLocked,
	} {
		if !s.IsValid() {
			t.Errorf("%v should be valid", s)
		}
	}
	if publicqueue.QueueState(99).IsValid() {
		t.Error("invalid state should not be valid")
	}
}

func TestQueue_ParamsClone(t *testing.T) {
	params := publicqueue.MustQueueParamsWithNS("orders", "production")
	clone := params.Clone()
	if clone == nil {
		t.Fatal("Clone returned nil")
	}
	if clone == params {
		t.Error("Clone should return a new instance")
	}
	if clone.Name() != params.Name() || clone.NS() != params.NS() {
		t.Errorf("Clone fields mismatch: got %s/%s, want %s/%s",
			clone.Name(), clone.NS(), params.Name(), params.NS())
	}
}

func TestQueue_RateLimitParamsString(t *testing.T) {
	rl := publicqueue.MustRateLimitParams(100, time.Minute)
	if rl.String() == "" {
		t.Error("RateLimitParams.String() returned empty string")
	}
}

func TestQueue_PurgeJobQueueParams(t *testing.T) {
	queueParams := publicqueue.MustQueueParams("orders")
	job := &publicqueue.PurgeJob{
		Payload: publicqueue.PurgeJobPayload{Queue: queueParams},
	}
	if job.QueueParams() != queueParams {
		t.Error("Params returned unexpected value")
	}
}

func TestQueue_CreateWithRateLimit(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-create-with-rate-limit")
	rl := publicqueue.MustRateLimitParams(10, time.Second)

	qm := redissmq.NewQueueManager()
	if err := qm.CreateWithRateLimit(ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint, rl); err != nil {
		t.Fatalf("CreateWithRateLimit: %v", err)
	}

	if err := qm.MustExist(ctx, params); err != nil {
		t.Fatalf("MustExist: %v", err)
	}
	if err := qm.MustBeOperational(ctx, params); err != nil {
		t.Fatalf("MustBeOperational: %v", err)
	}
	if err := qm.CanEnqueue(ctx, params); err != nil {
		t.Fatalf("CanEnqueue: %v", err)
	}
	if err := qm.CanDequeue(ctx, params); err != nil {
		t.Fatalf("CanDequeue: %v", err)
	}
}
