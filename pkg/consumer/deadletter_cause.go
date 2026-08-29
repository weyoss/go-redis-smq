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

// DeadLetterCause represents the reason a message was dead‑lettered.
type DeadLetterCause int

const (
	// DeadLetterTTLExpired indicates the message TTL expired.
	DeadLetterTTLExpired DeadLetterCause = 0
	// DeadLetterRetryThresholdExceeded indicates the retry threshold was exceeded.
	DeadLetterRetryThresholdExceeded DeadLetterCause = 1
	// DeadLetterPeriodicMessage indicates the message was periodic and not retried.
	DeadLetterPeriodicMessage DeadLetterCause = 2
)
