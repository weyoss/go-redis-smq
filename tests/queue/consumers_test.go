/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	redissmq "github.com/weyoss/go-redis-smq"
	"github.com/weyoss/go-redis-smq/internal/testutil"
	msg "github.com/weyoss/go-redis-smq/pkg/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Scenario: GetConsumers / GetConsumerIds on an existing queue with no
// consumers must return empty results (not errors).
func TestConsumers_EmptyQueue(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-consumers-empty")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	qm := redissmq.NewQueueManager()

	consumers, err := qm.GetConsumers(ctx, params)
	if err != nil {
		t.Fatalf("get consumers: %v", err)
	}
	if len(consumers) != 0 {
		t.Errorf("consumers = %d, want 0", len(consumers))
	}

	ids, err := qm.GetConsumerIds(ctx, params)
	if err != nil {
		t.Fatalf("get consumer ids: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("consumer ids = %d, want 0", len(ids))
	}
}

// Scenario: A registered consumer appears in both GetConsumers and
// GetConsumerIds. The QueueConsumer fields are populated from the
// consumer's subscription payload (hostname, PID, createdAt, IP addresses).
//
// IPAddresses is asserted against the real localIPs behavior:
//   - non-nil
//   - every entry is a syntactically valid IPv4 address
//   - no loopback (127.0.0.0/8) entries — filtered by localIPs
//   - wire format is a JSON array, never null
//
// On a host with no non-internal IPv4 (isolated container with only
// loopback), IPAddresses is an empty (non-nil) slice and the per-entry
// checks pass vacuously — the test still holds.
func TestConsumers_SingleConsumer(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-consumers-single")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons := redissmq.NewConsumer()
	cons.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	qm := redissmq.NewQueueManager()

	consumers, err := qm.GetConsumers(ctx, params)
	if err != nil {
		t.Fatalf("get consumers: %v", err)
	}
	if len(consumers) != 1 {
		t.Fatalf("consumers = %d, want 1", len(consumers))
	}

	consumer, ok := consumers[cons.ID()]
	if !ok {
		t.Fatalf("consumer %s not found in map: %v", cons.ID(), consumers)
	}
	if consumer.Hostname == "" {
		t.Errorf("hostname should not be empty")
	}
	if consumer.PID == 0 {
		t.Errorf("pid should not be 0")
	}
	if consumer.CreatedAt == 0 {
		t.Errorf("createdAt should not be 0")
	}

	// IPAddresses must be a non-nil slice.
	if consumer.IPAddresses == nil {
		t.Errorf("IPAddresses should be non-nil")
	}

	// Every entry must be a valid, non-loopback IPv4 address.
	for _, ipStr := range consumer.IPAddresses {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			t.Errorf("invalid IP in IPAddresses: %q", ipStr)
			continue
		}
		if ip.To4() == nil {
			t.Errorf("non-IPv4 address %q should be filtered out", ipStr)
		}
		if ip.IsLoopback() {
			t.Errorf("loopback address %q should be filtered out", ipStr)
		}
	}

	// Wire-format assertion: ipAddress must serialize as a JSON array,
	// never null.
	data, err := json.Marshal(consumer)
	if err != nil {
		t.Fatalf("marshal consumer: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal consumer: %v", err)
	}
	ipField, ok := fields["ipAddress"]
	if !ok {
		t.Fatalf("ipAddress field missing from serialized consumer: %s", data)
	}
	if string(ipField) == "null" {
		t.Errorf("ipAddress serialized as null; expected an array: %s", data)
	}
	if !strings.HasPrefix(string(ipField), "[") {
		t.Errorf("ipAddress should serialize as a JSON array, got: %s", ipField)
	}

	ids, err := qm.GetConsumerIds(ctx, params)
	if err != nil {
		t.Fatalf("get consumer ids: %v", err)
	}
	if len(ids) != 1 || ids[0] != cons.ID() {
		t.Errorf("consumer ids = %v, want [%s]", ids, cons.ID())
	}
}

// Scenario: Multiple consumers each appear with their distinct ID.
func TestConsumers_MultipleConsumers(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-consumers-multi")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPointToPoint)

	cons1 := redissmq.NewConsumer()
	cons1.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons1.Run(ctx); err != nil {
		t.Fatalf("run consumer 1: %v", err)
	}
	defer cons1.Shutdown()

	cons2 := redissmq.NewConsumer()
	cons2.Consume(params, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons2.Run(ctx); err != nil {
		t.Fatalf("run consumer 2: %v", err)
	}
	defer cons2.Shutdown()

	qm := redissmq.NewQueueManager()

	consumers, err := qm.GetConsumers(ctx, params)
	if err != nil {
		t.Fatalf("get consumers: %v", err)
	}
	if len(consumers) != 2 {
		t.Fatalf("consumers = %d, want 2", len(consumers))
	}

	if _, ok := consumers[cons1.ID()]; !ok {
		t.Errorf("consumer 1 (%s) not found", cons1.ID())
	}
	if _, ok := consumers[cons2.ID()]; !ok {
		t.Errorf("consumer 2 (%s) not found", cons2.ID())
	}

	ids, err := qm.GetConsumerIds(ctx, params)
	if err != nil {
		t.Fatalf("get consumer ids: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("consumer ids = %d, want 2", len(ids))
	}
}

// Scenario: Both methods must return ErrNotFound for a non-existent
// queue.
func TestConsumers_QueueNotFound(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-consumers-not-found")

	qm := redissmq.NewQueueManager()

	_, err := qm.GetConsumers(ctx, params)
	if err == nil {
		t.Fatal("GetConsumers: expected error for non-existent queue")
	}
	if !errors.Is(err, publicqueue.ErrNotFound) {
		t.Errorf("GetConsumers: err = %v, want ErrNotFound", err)
	}

	_, err = qm.GetConsumerIds(ctx, params)
	if err == nil {
		t.Fatal("GetConsumerIds: expected error for non-existent queue")
	}
	if !errors.Is(err, publicqueue.ErrNotFound) {
		t.Errorf("GetConsumerIds: err = %v, want ErrNotFound", err)
	}
}

// Scenario: A Pub/Sub queue with a group-scoped consumer must also
// surface it via GetConsumers. Regression guard against a future
// change that filters by delivery model.
func TestConsumers_PubSubConsumer(t *testing.T) {
	ctx := testutil.Setup(t)

	params := publicqueue.MustQueueParams("test-consumers-pubsub")
	testutil.CreateQueue(t, ctx, params, publicqueue.TypeFIFO, publicqueue.DeliveryPubSub)

	const groupID = "workers"
	cgm := redissmq.NewConsumerGroupManager()
	if _, err := cgm.Save(ctx, params, groupID); err != nil {
		t.Fatalf("create group: %v", err)
	}

	cons := redissmq.NewConsumer()
	cons.ConsumeWithGroup(params, groupID, func(ctx context.Context, m *msg.Transferable) error { return nil })
	if err := cons.Run(ctx); err != nil {
		t.Fatalf("run consumer: %v", err)
	}
	defer cons.Shutdown()

	qm := redissmq.NewQueueManager()

	consumers, err := qm.GetConsumers(ctx, params)
	if err != nil {
		t.Fatalf("get consumers: %v", err)
	}
	if len(consumers) != 1 {
		t.Fatalf("consumers = %d, want 1", len(consumers))
	}

	if _, ok := consumers[cons.ID()]; !ok {
		t.Errorf("consumer %s not found in map for Pub/Sub queue", cons.ID())
	}
}
