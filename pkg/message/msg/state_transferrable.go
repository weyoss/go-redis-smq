/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package msg

// StateTransferable holds the message state for serialization.
type StateTransferable struct {
	UUID                     string `json:"uuid"`
	ScheduledAt              *int64 `json:"scheduledAt,omitempty"`
	PublishedAt              *int64 `json:"publishedAt,omitempty"`
	RequeuedAt               *int64 `json:"requeuedAt,omitempty"`
	ProcessingStartedAt      *int64 `json:"processingStartedAt,omitempty"`
	AcknowledgedAt           *int64 `json:"acknowledgedAt,omitempty"`
	UnacknowledgedAt         *int64 `json:"unacknowledgedAt,omitempty"`
	DeadLetteredAt           *int64 `json:"deadLetteredAt,omitempty"`
	LastRequeuedAt           *int64 `json:"lastRequeuedAt,omitempty"`
	LastUnacknowledgedAt     *int64 `json:"lastUnacknowledgedAt,omitempty"`
	LastScheduledAt          *int64 `json:"lastScheduledAt,omitempty"`
	LastRetriedAttemptAt     *int64 `json:"lastRetriedAttemptAt,omitempty"`
	LastProcessedAt          *int64 `json:"lastProcessedAt,omitempty"`
	ScheduledCronFired       bool   `json:"scheduledCronFired"`
	Attempts                 int    `json:"attempts"`
	ScheduledRepeatCount     int    `json:"scheduledRepeatCount"`
	RequeueCount             int    `json:"requeueCount"`
	Expired                  bool   `json:"expired"`
	EffectiveScheduledDelay  int64  `json:"effectiveScheduledDelay"`
	ScheduledTimes           int    `json:"scheduledTimes"`
	ScheduledMessageParentID string `json:"scheduledMessageParentId,omitempty"`
	RequeuedMessageParentID  string `json:"requeuedMessageParentId,omitempty"`
}
