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

// ConsumeOptions defines default message consumption behavior.
type ConsumeOptions struct {
	// TTL is the time-to-live in milliseconds (0 = no expiration).
	TTL int64 `json:"ttl"`

	// RetryThreshold is the maximum retry attempts (0 = no retries).
	RetryThreshold int `json:"retryThreshold"`

	// RetryDelay is the delay between retries in milliseconds.
	RetryDelay int64 `json:"retryDelay"`

	// ConsumeTimeout is the maximum consumption time in milliseconds (0 = no timeout).
	ConsumeTimeout int64 `json:"consumeTimeout"`
}

// DefaultConsumeOptions returns sensible defaults.
func DefaultConsumeOptions() ConsumeOptions {
	return ConsumeOptions{
		TTL:            0,
		RetryThreshold: 3,
		RetryDelay:     60000,
		ConsumeTimeout: 0,
	}
}
