/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package schema

import "strconv"

type Field int

const (
	ID Field = iota
	Status
	Message
	ScheduledAt
	PublishedAt
	ProcessingStartedAt
	DeadLetteredAt
	AcknowledgedAt
	UnacknowledgedAt
	LastUnacknowledgedAt
	LastScheduledAt
	LastProcessedAt
	RequeuedAt
	RequeueCount
	LastRequeuedAt
	LastRetriedAttemptAt
	ScheduledCronFired
	Attempts
	ScheduledRepeatCount
	Expired
	EffectiveScheduledDelay
	ScheduledTimes
	ScheduledMessageParentID
	RequeuedMessageParentID
)

func (p Field) Key() string { return strconv.Itoa(int(p)) }
func (p Field) Int() int    { return int(p) }
