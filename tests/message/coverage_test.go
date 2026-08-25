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

	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
)

func TestMessage_StateTimestampAccessors(t *testing.T) {
	state := publicmessage.NewMessageState()
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
	cases := map[publicmessage.MessagePriority]string{
		publicmessage.PriorityHighest:     "highest",
		publicmessage.PriorityVeryHigh:    "very_high",
		publicmessage.PriorityHigh:        "high",
		publicmessage.PriorityAboveNormal: "above_normal",
		publicmessage.PriorityNormal:      "normal",
		publicmessage.PriorityLow:         "low",
		publicmessage.PriorityVeryLow:     "very_low",
		publicmessage.PriorityLowest:      "lowest",
	}

	for priority, expected := range cases {
		if priority.String() != expected {
			t.Errorf("%v.String() = %q, want %q", priority, priority.String(), expected)
		}
		if !priority.IsValid() {
			t.Errorf("%v should be valid", priority)
		}
	}

	if publicmessage.MessagePriority(99).String() != "unknown" {
		t.Errorf("invalid priority string = %q", publicmessage.MessagePriority(99).String())
	}
	if publicmessage.MessagePriority(99).IsValid() {
		t.Error("invalid priority should not be valid")
	}
}

func TestMessage_StatusStringAndPredicates(t *testing.T) {
	if !publicmessage.StatusAcknowledged.IsTerminal() || !publicmessage.StatusDeadLettered.IsTerminal() {
		t.Error("acknowledged and dead lettered should be terminal")
	}
	if !publicmessage.StatusPending.IsPending() {
		t.Error("pending should be pending")
	}
	if !publicmessage.StatusProcessing.IsProcessing() {
		t.Error("processing should be processing")
	}
	if !publicmessage.StatusAcknowledged.IsRequeuable() || !publicmessage.StatusDeadLettered.IsRequeuable() {
		t.Error("acknowledged and dead lettered should be requeuable")
	}

	for _, status := range []publicmessage.MessageStatus{
		publicmessage.StatusNew, publicmessage.StatusPending, publicmessage.StatusProcessing, publicmessage.StatusScheduled,
		publicmessage.StatusAcknowledged, publicmessage.StatusUnackRequeuing, publicmessage.StatusUnackDelaying,
		publicmessage.StatusDeadLettered,
	} {
		if !status.IsValid() {
			t.Errorf("%v should be valid", status)
		}
	}

	if publicmessage.MessageStatus(99).IsValid() {
		t.Error("invalid status should not be valid")
	}
}

func TestMessage_StateAccessors(t *testing.T) {
	state := publicmessage.NewMessageState()

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
