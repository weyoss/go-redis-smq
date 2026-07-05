/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events

import (
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

const (
	// Lifecycle
	EventUp        = "consumer.up"
	EventDown      = "consumer.down"
	EventGoingUp   = "consumer.goingUp"
	EventGoingDown = "consumer.goingDown"

	// Consume
	EventMessageReceived       = "consumer.messageReceived"
	EventMessageAcknowledged   = "consumer.messageAcknowledged"
	EventMessageUnacknowledged = "consumer.messageUnacknowledged"
	EventMessageDeadLettered   = "consumer.messageDeadLettered"
	EventMessageRequeued       = "consumer.messageRequeued"
	EventMessageDelayed        = "consumer.messageDelayed"
)

// ── Lifecycle ──

type LifecyclePayload struct {
	ConsumerID string `json:"consumerId"`
}

// ── Message ──

type MessagePayload struct {
	MessageID        string        `json:"messageId"`
	Queue            q.QueueParams `json:"queue"`
	GroupID          string        `json:"groupId,omitempty"`
	MessageHandlerID string        `json:"messageHandlerId,omitempty"`
	ConsumerID       string        `json:"consumerId"`
}

type MessageUnacknowledgedPayload struct {
	MessagePayload
	Cause int `json:"cause"`
}

type MessageDeadLetteredPayload struct {
	MessagePayload
	Cause int `json:"cause"`
}

type MessageReceivedPayload struct {
	MessageID  string        `json:"messageId"`
	Queue      q.QueueParams `json:"queue"`
	ConsumerID string        `json:"consumerId"`
}
