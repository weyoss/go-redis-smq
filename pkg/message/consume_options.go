/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message

// ConsumeOptions defines default message consumption behaviour.
//
// These defaults are applied to every new message created with New()
// unless overridden by calling SetDefaultConsumeOptions.
type ConsumeOptions struct {
	// TTL is the default time-to-live in milliseconds.
	// A value of 0 means messages do not expire.
	TTL int64 `json:"ttl"`

	// RetryThreshold is the default maximum number of retry attempts.
	// A value of 0 means messages are not retried.
	RetryThreshold int `json:"retryThreshold"`

	// RetryDelay is the default delay between retries in milliseconds.
	RetryDelay int64 `json:"retryDelay"`

	// ConsumeTimeout is the default maximum consumption time in milliseconds.
	// A value of 0 means no timeout is enforced.
	ConsumeTimeout int64 `json:"consumeTimeout"`
}

// DefaultConsumeOptions returns sensible default consumption options.
//
// The defaults are:
//   - TTL: 0 (no expiration)
//   - RetryThreshold: 3
//   - RetryDelay: 60000 ms (1 minute)
//   - ConsumeTimeout: 0 (no timeout)
func DefaultConsumeOptions() ConsumeOptions {
	return ConsumeOptions{
		TTL:            0,
		RetryThreshold: 3,
		RetryDelay:     60000,
		ConsumeTimeout: 0,
	}
}
