/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer_test

import (
	"testing"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Scenario: Producer fails to publish to stopped queue
func TestError_ProduceToStoppedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-produce-stopped")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	queue.Stop(ctx, params, nil)

	prod := testutil.StartProducer(t, ctx)
	_, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	if err == nil {
		t.Fatal("expected error: cannot produce to stopped queue")
	}
}

// Scenario: Producer fails to publish to locked queue
func TestError_ProduceToLockedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := q.MustQueueParams("test-error-produce-locked")
	testutil.CreateQueue(t, ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

	queue.Lock(ctx, params, q.LockOwnerPurgeJob, "lock-123", nil)

	prod := testutil.StartProducer(t, ctx)
	_, err := prod.Produce(ctx, msg.New().SetBody("msg").SetQueue(params))
	if err == nil {
		t.Fatal("expected error: cannot produce to locked queue")
	}
}
