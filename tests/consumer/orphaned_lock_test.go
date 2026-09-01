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
	"context"
	"testing"
	"time"

	redissmq "github.com/weyoss/go-redis-smq"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

func TestOrphanedLockRecoverer_UnlocksQueueWhenJobDone(t *testing.T) {
	ctx := testutil.Setup(t)
	params := queue.MustQueueParams("test-orphaned-lock-unlock-when-done")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	sm := redissmq.NewStateManager()
	lockID := "orphaned-job"

	// Simulate an orphaned purge lock.
	if _, err := sm.Lock(ctx, params, queue.LockOwnerPurgeJob, lockID, nil); err != nil {
		t.Fatal(err)
	}

	recoverer := internalconsumer.NewOrphanedLockRecoverer(
		params,
		"test-consumer",
		internalconsumer.WithOrphanedLockRecoverInterval(10*time.Second),
	)
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	recoverer.Run(ctx2)

	// Wait for the recoverer to unlock the queue.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		tr, err := sm.Current(ctx, params)
		if err == nil && tr.To == queue.StateActive {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatal("queue was not unlocked within 15 seconds")
}
