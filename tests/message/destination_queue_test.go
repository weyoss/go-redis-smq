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
	"encoding/json"
	"strings"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: Transferable.DestinationQueue must always be a valid queue.Params
// — non-nullable, both at the Go type level and on the JSON wire format.
// Pre-fix, DestinationQueue was *queue.Params and could serialize as null,
// which crashes TypeScript consumers doing msg.destinationQueue.name.
func TestDestinationQueue_NonNullable(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-1-4-destination-nonnull")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	prod := testutil.StartProducer(t, ctx)
	ids, err := prod.Produce(ctx, msg.New().SetBody("x").SetQueue(params))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	m, err := redissmq.NewMessageManager().Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// DestinationQueue must be a valid, populated queue.
	if m.DestinationQueue.Name() == "" {
		t.Fatal("DestinationQueue.Name() = \"\"; a routed message must have a valid destination")
	}
	if m.DestinationQueue.Name() != params.Name() {
		t.Errorf("DestinationQueue.Name() = %q, want %q",
			m.DestinationQueue.Name(), params.Name())
	}
	if m.DestinationQueue.NS() != params.NS() {
		t.Errorf("DestinationQueue.NS() = %q, want %q",
			m.DestinationQueue.NS(), params.NS())
	}

	// Wire format must have destinationQueue as a JSON object — never null.
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(data)

	if strings.Contains(out, `"destinationQueue":null`) {
		t.Errorf("destinationQueue must never serialize as null (breaks TS .name access): %s", out)
	}
	if !strings.Contains(out, `"destinationQueue":{"name":`) {
		t.Errorf("destinationQueue should be a {name, ns} object: %s", out)
	}
}
