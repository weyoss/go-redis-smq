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

import "errors"

var (
	ErrNotFound                   = errors.New("message not found")
	ErrNotRequeuable              = errors.New("message is not requeuable")
	ErrExchangeRequired           = errors.New("exchange required for routing key")
	ErrDestinationQueueAlreadySet = errors.New("destination queue already set")
	ErrDestinationQueueRequired   = errors.New("destination queue required")
	ErrInvalidPriority            = errors.New("invalid message priority")
	ErrInvalidCronExpression      = errors.New("invalid cron expression")
	ErrMessageExpired             = errors.New("message has expired")
	ErrRetryThresholdExceeded     = errors.New("retry threshold exceeded")
)
