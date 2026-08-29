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

// UnacknowledgeCause represents the reason a message was unacknowledged.
type UnacknowledgeCause int

const (
	// CauseTimeout indicates the consume timeout was exceeded.
	CauseTimeout UnacknowledgeCause = 0
	// CauseConsumeError indicates the handler returned an error.
	CauseConsumeError UnacknowledgeCause = 1
	// CauseUnacknowledged indicates the message was explicitly unacknowledged.
	CauseUnacknowledged UnacknowledgeCause = 2
	// CauseOfflineConsumer indicates the consumer was detected offline.
	CauseOfflineConsumer UnacknowledgeCause = 3
	// CauseShuttingDown indicates the consumer shut down while processing.
	CauseShuttingDown UnacknowledgeCause = 4
	// CauseTTLExpired indicates the message TTL expired.
	CauseTTLExpired UnacknowledgeCause = 5
	// CauseQueueStopped indicates the queue was stopped.
	CauseQueueStopped UnacknowledgeCause = 6
	// CauseQueueInvalidState indicates the queue was in an invalid state.
	CauseQueueInvalidState UnacknowledgeCause = 7
	// CauseQueueLocked indicates the queue was locked.
	CauseQueueLocked UnacknowledgeCause = 8
	// CauseMessageNotFound indicates the message no longer exists.
	CauseMessageNotFound UnacknowledgeCause = 9
	// CauseQueueStateChanged indicates the queue state changed during processing.
	CauseQueueStateChanged UnacknowledgeCause = 10
	// CauseQueueNotFound indicates the queue no longer exists.
	CauseQueueNotFound UnacknowledgeCause = 11
	// CauseUnexpectedError indicates an unexpected internal error.
	CauseUnexpectedError UnacknowledgeCause = 12
	// CauseInvalidHandlerSignature indicates the handler signature was invalid.
	CauseInvalidHandlerSignature UnacknowledgeCause = 13
)
