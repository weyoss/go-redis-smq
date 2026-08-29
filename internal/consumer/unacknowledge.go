/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package consumer

import (
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
)

type UnacknowledgeAction int

const (
	ActionDeadLetter UnacknowledgeAction = 0
	ActionRequeue    UnacknowledgeAction = 1
	ActionDelay      UnacknowledgeAction = 2
)

func (a UnacknowledgeAction) String() string {
	switch a {
	case ActionDeadLetter:
		return "DEAD_LETTER"
	case ActionRequeue:
		return "REQUEUE"
	case ActionDelay:
		return "DELAY"
	default:
		return "UNKNOWN"
	}
}

// resolveUnackAction determines the action and dead-letter cause for a failed message.
func resolveUnackAction(msg *internalMessage.Envelope, cause consumer.UnacknowledgeCause) (UnacknowledgeAction, consumer.DeadLetterCause) {
	log := logger.New("consumer", "unacknowledge", msg.ID())

	if cause == consumer.CauseTTLExpired || msg.IsExpired() {
		log.Debug("message expired — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
		)
		return ActionDeadLetter, consumer.DeadLetterTTLExpired
	}

	if msg.IsPeriodic() {
		log.Debug("periodic message — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
		)
		return ActionDeadLetter, consumer.DeadLetterPeriodicMessage
	}

	if msg.HasRetryThresholdExceeded() {
		log.Debug("retry threshold exceeded — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
			"threshold", msg.ProducibleMessage().RetryThreshold(),
		)
		return ActionDeadLetter, consumer.DeadLetterRetryThresholdExceeded
	}

	if msg.ProducibleMessage().RetryDelay() > 0 {
		log.Debug("scheduling delayed retry",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
			"delay", msg.ProducibleMessage().RetryDelay(),
		)
		return ActionDelay, 0
	}

	log.Debug("requeuing message immediately",
		"cause", int(cause),
		"attempts", msg.MessageState().Attempts(),
	)
	return ActionRequeue, 0
}
