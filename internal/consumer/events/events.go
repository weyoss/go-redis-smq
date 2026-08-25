/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package events defines internal consumer event names and payload types.
//
// These events are published through the event multiplexer to the
// appropriate bus (system or user) according to the event routing policy.
package events

import (
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// Consumer event names.
const (
	// Lifecycle
	EventUp        = "consumer.up"
	EventDown      = "consumer.down"
	EventGoingUp   = "consumer.goingUp"
	EventGoingDown = "consumer.goingDown"

	// Message processing
	EventMessageReceived       = "consumer.messageReceived"
	EventMessageAcknowledged   = "consumer.messageAcknowledged"
	EventMessageUnacknowledged = "consumer.messageUnacknowledged"
	EventMessageDeadLettered   = "consumer.messageDeadLettered"
	EventMessageRequeued       = "consumer.messageRequeued"
	EventMessageDelayed        = "consumer.messageDelayed"
)

// ── Lifecycle payloads ──

// LifecyclePayload is used by public subscribers to receive the consumer ID
// from consumer lifecycle events.
type LifecyclePayload struct {
	ConsumerID string `json:"consumerId"`
}

// ── Message payloads ──

// MessagePayload is the base payload for consumer message events.
type MessagePayload struct {
	MessageID  string            `json:"messageId"`
	Queue      queue.QueueParams `json:"queue"`
	GroupID    string            `json:"groupId,omitempty"`
	ConsumerID string            `json:"consumerId"`
}

// MessageUnacknowledgedPayload is used for consumer.messageUnacknowledged.
type MessageUnacknowledgedPayload struct {
	MessagePayload
	Cause int `json:"cause"`
}

// MessageDeadLetteredPayload is used for consumer.messageDeadLettered.
type MessageDeadLetteredPayload struct {
	MessagePayload
	Cause int `json:"cause"`
}

// MessageReceivedPayload is used for consumer.messageReceived.
type MessageReceivedPayload struct {
	MessageID  string            `json:"messageId"`
	Queue      queue.QueueParams `json:"queue"`
	ConsumerID string            `json:"consumerId"`
}
