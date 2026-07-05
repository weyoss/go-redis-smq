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
)

type UnacknowledgeCause int

const (
	CauseTimeout                 UnacknowledgeCause = 0  // TIMEOUT
	CauseConsumeError            UnacknowledgeCause = 1  // CONSUME_ERROR
	CauseUnacknowledged          UnacknowledgeCause = 2  // UNACKNOWLEDGED
	CauseOfflineConsumer         UnacknowledgeCause = 3  // OFFLINE_CONSUMER
	CauseShuttingDown            UnacknowledgeCause = 4  // SHUTTING_DOWN
	CauseTTLExpired              UnacknowledgeCause = 5  // TTL_EXPIRED
	CauseQueueStopped            UnacknowledgeCause = 6  // QUEUE_STOPPED
	CauseQueueInvalidState       UnacknowledgeCause = 7  // QUEUE_INVALID_STATE
	CauseQueueLocked             UnacknowledgeCause = 8  // QUEUE_LOCKED
	CauseMessageNotFound         UnacknowledgeCause = 9  // MESSAGE_NOT_FOUND
	CauseQueueStateChanged       UnacknowledgeCause = 10 // QUEUE_STATE_CHANGED
	CauseQueueNotFound           UnacknowledgeCause = 11 // QUEUE_NOT_FOUND
	CauseUnexpectedError         UnacknowledgeCause = 12 // UNEXPECTED_ERROR
	CauseInvalidHandlerSignature UnacknowledgeCause = 13 // INVALID_HANDLER_SIGNATURE
)

type DeadLetterCause int

const (
	DeadLetterTTLExpired             DeadLetterCause = 0
	DeadLetterRetryThresholdExceeded DeadLetterCause = 1
	DeadLetterPeriodicMessage        DeadLetterCause = 2
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
func resolveUnackAction(msg *internalMessage.Envelope, cause UnacknowledgeCause) (UnacknowledgeAction, DeadLetterCause) {
	log := logger.New("consumer", "unacknowledge", msg.ID())

	if cause == CauseTTLExpired || msg.IsExpired() {
		log.Debug("message expired — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
		)
		return ActionDeadLetter, DeadLetterTTLExpired
	}

	if msg.IsPeriodic() {
		log.Debug("periodic message — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
		)
		return ActionDeadLetter, DeadLetterPeriodicMessage
	}

	if msg.HasRetryThresholdExceeded() {
		log.Debug("retry threshold exceeded — dead lettering",
			"cause", int(cause),
			"attempts", msg.MessageState().Attempts(),
			"threshold", msg.ProducibleMessage().RetryThreshold(),
		)
		return ActionDeadLetter, DeadLetterRetryThresholdExceeded
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
