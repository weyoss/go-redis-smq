/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import (
	"encoding/json"
	"time"
)

// Props holds the stored configuration of a queue.
//
// The JSON wire format matches TypeScript's IQueueProperties interface
// exactly — see MarshalJSON. Field access remains Go-idiomatic; the wire
// format is what crosses the language boundary.
type Props struct {
	// Type is the queue ordering type (FIFO, LIFO, or Priority).
	Type Type

	// DeliveryModel is the delivery model (Point-to-Point or Pub/Sub).
	DeliveryModel DeliveryModel

	// OperationalState is the current operational state of the queue.
	OperationalState State

	// MessagesCount is the total number of messages in the queue.
	MessagesCount int64

	// ScheduledMessagesCount is the number of messages scheduled for future delivery.
	ScheduledMessagesCount int64

	// PendingMessagesCount is the number of messages waiting to be consumed.
	PendingMessagesCount int64

	// ProcessingMessagesCount is the number of messages currently being processed.
	ProcessingMessagesCount int64

	// AcknowledgedMessagesCount is the number of successfully processed messages.
	AcknowledgedMessagesCount int64

	// DeadLetteredMessagesCount is the number of messages that failed permanently.
	DeadLetteredMessagesCount int64

	// DelayedMessagesCount is the number of messages waiting for a retry delay.
	DelayedMessagesCount int64

	// RequeuedMessagesCount is the number of messages waiting to be re-inserted.
	RequeuedMessagesCount int64

	// RateLimit is the rate limit configuration, or nil if none is set.
	RateLimit *RateLimitParams

	// LastStateChangeAt is the timestamp of the last state transition.
	// Zero if unknown (serialized as null on the wire).
	LastStateChangeAt time.Time

	// LockID is the lock identifier when the queue is locked.
	// Empty when unlocked (serialized as null on the wire).
	LockID string
}

// MarshalJSON produces a wire representation matching TypeScript's
// IQueueProperties interface:
//
//	{
//	  "queueType": <int>,
//	  "deliveryModel": <int>,
//	  "operationalState": <int>,
//	  "messagesCount": <int>,
//	  "scheduledMessagesCount": <int>,
//	  "pendingMessagesCount": <int>,
//	  "processingMessagesCount": <int>,
//	  "acknowledgedMessagesCount": <int>,
//	  "deadLetteredMessagesCount": <int>,
//	  "delayedMessagesCount": <int>,
//	  "requeuedMessagesCount": <int>,
//	  "rateLimit": {"limit": <int>, "interval": <ms>} | null,
//	  "lastStateChangeAt": <unix-ms> | null,
//	  "lockId": "<string>" | null
//	}
//
// Empty values are serialized as null rather than zero/empty-string,
// matching the TypeScript interface's nullable fields.
//
// The anonymous struct is used (rather than struct tags on Props) so that
// Go-idiomatic field names remain available to callers while the wire
// format stays foreign-friendly. Field order in the anonymous struct
// determines JSON key order, which is deterministic and tested.
func (p Props) MarshalJSON() ([]byte, error) {
	var lastStateChangeAt interface{}
	if !p.LastStateChangeAt.IsZero() {
		lastStateChangeAt = p.LastStateChangeAt.UnixMilli()
	}

	var lockID interface{}
	if p.LockID != "" {
		lockID = p.LockID
	}

	return json.Marshal(&struct {
		QueueType                 Type             `json:"queueType"`
		DeliveryModel             DeliveryModel    `json:"deliveryModel"`
		OperationalState          State            `json:"operationalState"`
		MessagesCount             int64            `json:"messagesCount"`
		ScheduledMessagesCount    int64            `json:"scheduledMessagesCount"`
		PendingMessagesCount      int64            `json:"pendingMessagesCount"`
		ProcessingMessagesCount   int64            `json:"processingMessagesCount"`
		AcknowledgedMessagesCount int64            `json:"acknowledgedMessagesCount"`
		DeadLetteredMessagesCount int64            `json:"deadLetteredMessagesCount"`
		DelayedMessagesCount      int64            `json:"delayedMessagesCount"`
		RequeuedMessagesCount     int64            `json:"requeuedMessagesCount"`
		RateLimit                 *RateLimitParams `json:"rateLimit"`
		LastStateChangeAt         interface{}      `json:"lastStateChangeAt"`
		LockID                    interface{}      `json:"lockId"`
	}{
		QueueType:                 p.Type,
		DeliveryModel:             p.DeliveryModel,
		OperationalState:          p.OperationalState,
		MessagesCount:             p.MessagesCount,
		ScheduledMessagesCount:    p.ScheduledMessagesCount,
		PendingMessagesCount:      p.PendingMessagesCount,
		ProcessingMessagesCount:   p.ProcessingMessagesCount,
		AcknowledgedMessagesCount: p.AcknowledgedMessagesCount,
		DeadLetteredMessagesCount: p.DeadLetteredMessagesCount,
		DelayedMessagesCount:      p.DelayedMessagesCount,
		RequeuedMessagesCount:     p.RequeuedMessagesCount,
		RateLimit:                 p.RateLimit,
		LastStateChangeAt:         lastStateChangeAt,
		LockID:                    lockID,
	})
}

// UnmarshalJSON accepts both the current wire format (camelCase, matching
// TypeScript's IQueueProperties) and the legacy Go format (PascalCase,
// emitted by older Go versions that had no custom MarshalJSON).
//
// The dispatch is on presence of the "queueType" key: current format has
// it, legacy format has "Type". This lets a subscriber running the new
// code accept events from an emitter running the old code — the two-phase
// rolling upgrade path. Once every emitter is upgraded, only the camelCase
// path is exercised, but the legacy branch remains for backward
// compatibility with events already in flight or emitted by
// long-running processes on the old binary.
func (p *Props) UnmarshalJSON(data []byte) error {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	if _, isCurrent := probe["queueType"]; isCurrent {
		return p.unmarshalCurrent(data)
	}
	return p.unmarshalLegacy(data)
}

func (p *Props) unmarshalCurrent(data []byte) error {
	var aux struct {
		QueueType                 Type             `json:"queueType"`
		DeliveryModel             DeliveryModel    `json:"deliveryModel"`
		OperationalState          State            `json:"operationalState"`
		MessagesCount             int64            `json:"messagesCount"`
		ScheduledMessagesCount    int64            `json:"scheduledMessagesCount"`
		PendingMessagesCount      int64            `json:"pendingMessagesCount"`
		ProcessingMessagesCount   int64            `json:"processingMessagesCount"`
		AcknowledgedMessagesCount int64            `json:"acknowledgedMessagesCount"`
		DeadLetteredMessagesCount int64            `json:"deadLetteredMessagesCount"`
		DelayedMessagesCount      int64            `json:"delayedMessagesCount"`
		RequeuedMessagesCount     int64            `json:"requeuedMessagesCount"`
		RateLimit                 *RateLimitParams `json:"rateLimit"`
		LastStateChangeAt         *int64           `json:"lastStateChangeAt"`
		LockID                    *string          `json:"lockId"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	p.Type = aux.QueueType
	p.DeliveryModel = aux.DeliveryModel
	p.OperationalState = aux.OperationalState
	p.MessagesCount = aux.MessagesCount
	p.ScheduledMessagesCount = aux.ScheduledMessagesCount
	p.PendingMessagesCount = aux.PendingMessagesCount
	p.ProcessingMessagesCount = aux.ProcessingMessagesCount
	p.AcknowledgedMessagesCount = aux.AcknowledgedMessagesCount
	p.DeadLetteredMessagesCount = aux.DeadLetteredMessagesCount
	p.DelayedMessagesCount = aux.DelayedMessagesCount
	p.RequeuedMessagesCount = aux.RequeuedMessagesCount
	p.RateLimit = aux.RateLimit

	if aux.LastStateChangeAt != nil {
		p.LastStateChangeAt = time.UnixMilli(*aux.LastStateChangeAt)
	} else {
		p.LastStateChangeAt = time.Time{}
	}

	if aux.LockID != nil {
		p.LockID = *aux.LockID
	} else {
		p.LockID = ""
	}

	return nil
}

func (p *Props) unmarshalLegacy(data []byte) error {
	var aux struct {
		Type                      Type             `json:"Type"`
		DeliveryModel             DeliveryModel    `json:"DeliveryModel"`
		OperationalState          State            `json:"OperationalState"`
		MessagesCount             int64            `json:"MessagesCount"`
		ScheduledMessagesCount    int64            `json:"ScheduledMessagesCount"`
		PendingMessagesCount      int64            `json:"PendingMessagesCount"`
		ProcessingMessagesCount   int64            `json:"ProcessingMessagesCount"`
		AcknowledgedMessagesCount int64            `json:"AcknowledgedMessagesCount"`
		DeadLetteredMessagesCount int64            `json:"DeadLetteredMessagesCount"`
		DelayedMessagesCount      int64            `json:"DelayedMessagesCount"`
		RequeuedMessagesCount     int64            `json:"RequeuedMessagesCount"`
		RateLimit                 *RateLimitParams `json:"RateLimit"`
		LastStateChangeAt         time.Time        `json:"LastStateChangeAt"`
		LockID                    string           `json:"LockID"`
		// CreatedAt deliberately absent: the legacy emitter may include
		// it, but we don't need the value and unmarshal tolerates extra
		// fields by default.
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	p.Type = aux.Type
	p.DeliveryModel = aux.DeliveryModel
	p.OperationalState = aux.OperationalState
	p.MessagesCount = aux.MessagesCount
	p.ScheduledMessagesCount = aux.ScheduledMessagesCount
	p.PendingMessagesCount = aux.PendingMessagesCount
	p.ProcessingMessagesCount = aux.ProcessingMessagesCount
	p.AcknowledgedMessagesCount = aux.AcknowledgedMessagesCount
	p.DeadLetteredMessagesCount = aux.DeadLetteredMessagesCount
	p.DelayedMessagesCount = aux.DelayedMessagesCount
	p.RequeuedMessagesCount = aux.RequeuedMessagesCount
	p.RateLimit = aux.RateLimit
	p.LastStateChangeAt = aux.LastStateChangeAt
	p.LockID = aux.LockID

	return nil
}
