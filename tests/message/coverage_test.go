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

	"github.com/weyoss/go-redis-smq/pkg/message/msg"
)

func TestMessage_StateTimestampAccessors(t *testing.T) {
	state := msg.NewMessageState()
	now := time.Now().UnixMilli()

	state.SetAcknowledgedAt(now)
	if got := state.AcknowledgedAt(); got == nil || *got != now {
		t.Errorf("AcknowledgedAt = %v, want %d", got, now)
	}

	state.SetUnacknowledgedAt(now)
	if got := state.UnacknowledgedAt(); got == nil || *got != now {
		t.Errorf("UnacknowledgedAt = %v, want %d", got, now)
	}

	state.SetDeadLetteredAt(now)
	if got := state.DeadLetteredAt(); got == nil || *got != now {
		t.Errorf("DeadLetteredAt = %v, want %d", got, now)
	}

	state.SetLastRequeuedAt(now)
	if got := state.LastRequeuedAt(); got == nil || *got != now {
		t.Errorf("LastRequeuedAt = %v, want %d", got, now)
	}

	state.SetLastUnacknowledgedAt(now)
	if got := state.LastUnacknowledgedAt(); got == nil || *got != now {
		t.Errorf("LastUnacknowledgedAt = %v, want %d", got, now)
	}

	state.SetLastRetriedAttemptAt(now)
	if got := state.LastRetriedAttemptAt(); got == nil || *got != now {
		t.Errorf("LastRetriedAttemptAt = %v, want %d", got, now)
	}

	state.SetLastProcessedAt(now)
	if got := state.LastProcessedAt(); got == nil || *got != now {
		t.Errorf("LastProcessedAt = %v, want %d", got, now)
	}
}

func TestMessage_PriorityStringAndIsValid(t *testing.T) {
	cases := map[msg.MessagePriority]string{
		msg.PriorityHighest:     "highest",
		msg.PriorityVeryHigh:    "very_high",
		msg.PriorityHigh:        "high",
		msg.PriorityAboveNormal: "above_normal",
		msg.PriorityNormal:      "normal",
		msg.PriorityLow:         "low",
		msg.PriorityVeryLow:     "very_low",
		msg.PriorityLowest:      "lowest",
	}

	for priority, expected := range cases {
		if priority.String() != expected {
			t.Errorf("%v.String() = %q, want %q", priority, priority.String(), expected)
		}
		if !priority.IsValid() {
			t.Errorf("%v should be valid", priority)
		}
	}

	if msg.MessagePriority(99).String() != "unknown" {
		t.Errorf("invalid priority string = %q", msg.MessagePriority(99).String())
	}
	if msg.MessagePriority(99).IsValid() {
		t.Error("invalid priority should not be valid")
	}
}

func TestMessage_StatusStringAndPredicates(t *testing.T) {
	if !msg.StatusAcknowledged.IsTerminal() || !msg.StatusDeadLettered.IsTerminal() {
		t.Error("acknowledged and dead lettered should be terminal")
	}
	if !msg.StatusPending.IsPending() {
		t.Error("pending should be pending")
	}
	if !msg.StatusProcessing.IsProcessing() {
		t.Error("processing should be processing")
	}
	if !msg.StatusAcknowledged.IsRequeuable() || !msg.StatusDeadLettered.IsRequeuable() {
		t.Error("acknowledged and dead lettered should be requeuable")
	}

	for _, status := range []msg.MessageStatus{
		msg.StatusNew, msg.StatusPending, msg.StatusProcessing, msg.StatusScheduled,
		msg.StatusAcknowledged, msg.StatusUnackRequeuing, msg.StatusUnackDelaying,
		msg.StatusDeadLettered,
	} {
		if !status.IsValid() {
			t.Errorf("%v should be valid", status)
		}
	}

	if msg.MessageStatus(99).IsValid() {
		t.Error("invalid status should not be valid")
	}
}

func TestMessage_StateAccessors(t *testing.T) {
	state := msg.NewMessageState()

	state.IncrAttempts()
	if state.Attempts() != 1 {
		t.Errorf("attempts = %d, want 1", state.Attempts())
	}

	state.SetExpired(true)
	if !state.Expired() {
		t.Error("expired should be true")
	}

	state.MarkPublished()
	if state.PublishedAt() == nil {
		t.Error("publishedAt should be set")
	}

	state.SetProcessingStartedAt(time.Now().UnixMilli())
	if state.ProcessingStartedAt() == nil {
		t.Error("processingStartedAt should be set")
	}

	state.SetScheduledMessageParentID("parent")
	if state.ScheduledMessageParentID() != "parent" {
		t.Errorf("scheduled parent id = %q, want parent", state.ScheduledMessageParentID())
	}

	state.SetRequeuedMessageParentID("requeue")
	if state.RequeuedMessageParentID() != "requeue" {
		t.Errorf("requeue parent id = %q, want requeue", state.RequeuedMessageParentID())
	}
}
