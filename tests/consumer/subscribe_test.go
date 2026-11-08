/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package consumer_test

import (
	"strings"
	"testing"

	"github.com/weyoss/go-redis-smq"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: SubscribeConsumer rejects subscription to a non-active queue
// directly (not through MessageHandlerRunner.Run, whose isQueueActive
// pre-check is a different, earlier gate).
func TestSubscribe_RejectsStoppedQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-h6-subscribe-stopped")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	if _, err := redissmq.NewStateManager().Stop(ctx, params, nil); err != nil {
		t.Fatalf("stop queue: %v", err)
	}

	// Direct call — bypasses MessageHandlerRunner.Run's isQueueActive
	// pre-check, so the Lua operational-state check is what runs.
	err := internalconsumer.SubscribeConsumer(ctx, "test-h6-consumer", params, "")
	if err == nil {
		// If this fires, either the schema was reordered and the
		// hardcoded "11" now points at an empty field (silent-allow
		// bug), or SubscribeConsumer was changed to skip the check.
		t.Fatal("SubscribeConsumer accepted subscription to a stopped queue; " +
			"the operational-state check did not fire (see H6)")
	}
	if !strings.Contains(err.Error(), "queue not active") {
		t.Errorf("expected 'queue not active' error, got: %v", err)
	}
}
