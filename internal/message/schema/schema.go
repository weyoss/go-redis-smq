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

type MessageField int

const (
	MessageFieldID MessageField = iota
	MessageFieldStatus
	MessageFieldMessage
	MessageFieldScheduledAt
	MessageFieldPublishedAt
	MessageFieldProcessingStartedAt
	MessageFieldDeadLetteredAt
	MessageFieldAcknowledgedAt
	MessageFieldUnacknowledgedAt
	MessageFieldLastUnacknowledgedAt
	MessageFieldLastScheduledAt
	MessageFieldLastProcessedAt
	MessageFieldRequeuedAt
	MessageFieldRequeueCount
	MessageFieldLastRequeuedAt
	MessageFieldLastRetriedAttemptAt
	MessageFieldScheduledCronFired
	MessageFieldAttempts
	MessageFieldScheduledRepeatCount
	MessageFieldExpired
	MessageFieldEffectiveScheduledDelay
	MessageFieldScheduledTimes
	MessageFieldScheduledMessageParentID
	MessageFieldRequeuedMessageParentID
)

func (p MessageField) Key() string { return strconv.Itoa(int(p)) }
func (p MessageField) Int() int    { return int(p) }
