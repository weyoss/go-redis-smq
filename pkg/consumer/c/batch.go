/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package c provides configuration options and types for RedisSMQ consumers.
//
// This file contains batch configuration types for acknowledgments and
// unacknowledgments.
package c

import "time"

// BatchConfig configures batch acknowledgment or unacknowledgment.
type BatchConfig struct {
	// Enabled enables batching. Disabled means immediate operations.
	Enabled bool

	// BatchSize is the maximum number of messages per batch.
	// Default: 100
	BatchSize int

	// BatchTimeout is the maximum time to wait before flushing a partial
	// batch. Default: 10 seconds.
	BatchTimeout time.Duration
}

// DefaultBatchConfig returns sensible defaults for batch processing.
//
// The defaults are:
//   - Enabled: false
//   - BatchSize: 100
//   - BatchTimeout: 10 seconds
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		Enabled:      false,
		BatchSize:    100,
		BatchTimeout: 10 * time.Second,
	}
}
