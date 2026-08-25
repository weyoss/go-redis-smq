/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message_test

import (
	"testing"
	"time"

	"github.com/weyoss/go-redis-smq/internal/testutil"
	"github.com/weyoss/go-redis-smq/pkg/message"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Very long message ID is rejected
func TestEdge_VeryLongMessageID(t *testing.T) {
	ctx := testutil.Setup(t)

	longID := string(make([]byte, 1000))
	_, err := message.Get(ctx, longID)
	if err == nil {
		t.Fatal("expected error for invalid message ID")
	}
}

// Scenario: Get state of message after TTL expiry
func TestEdge_MessageStateAfterTTL(t *testing.T) {
	ctx := testutil.Setup(t)

	params := queue.MustQueueParams("test-edge-ttl-state")
	testutil.CreateQueue(t, ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, _ := prod.Produce(ctx, msg.New().
		SetBody("ttl-state").
		SetQueue(params).
		SetTTL(500*time.Millisecond),
	)

	time.Sleep(1 * time.Second)

	state, err := message.State(ctx, ids[0])
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	t.Logf("attempts: %d, expired: %v", state.Attempts, state.Expired)
}
