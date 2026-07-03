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
	EventUp        = "producer.up"
	EventDown      = "producer.down"
	EventGoingUp   = "producer.goingUp"
	EventGoingDown = "producer.goingDown"

	// Message
	EventMessagePublished = "producer.messagePublished"
)

// ── Lifecycle ──

type LifecyclePayload struct {
	ProducerID string `json:"producerId"`
}

// ── Message ──

type MessagePublishedPayload struct {
	MessageID  string        `json:"messageId"`
	Queue      q.QueueParams `json:"queue"`
	GroupID    string        `json:"groupId,omitempty"`
	ProducerID string        `json:"producerId"`
}
