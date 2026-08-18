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
// It includes the Options struct, functional options, and batch configuration
// types used when creating consumers via consumer.New.
package c

import "time"

// Options holds consumer configuration.
type Options struct {
	// HeartbeatTTL is the key expiry time for heartbeat keys.
	// Minimum 3 seconds, default 60 seconds.
	// Matches TypeScript heartbeatTTL.
	HeartbeatTTL time.Duration

	// BatchAcks configures batch acknowledgments.
	BatchAcks BatchConfig

	// BatchUnacks configures batch unacknowledgments.
	BatchUnacks BatchConfig
}

// Option is a functional option for configuring a Consumer.
type Option func(*Options)

// DefaultOptions returns sensible defaults.
func DefaultOptions() *Options {
	return &Options{
		HeartbeatTTL: 60 * time.Second,
		BatchAcks:    DefaultBatchConfig(),
		BatchUnacks:  DefaultBatchConfig(),
	}
}

// WithHeartbeatTTL sets the heartbeat key expiry time.
func WithHeartbeatTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.HeartbeatTTL = ttl
	}
}

// WithBatchAcks configures batch acknowledgments.
func WithBatchAcks(cfg BatchConfig) Option {
	return func(o *Options) {
		o.BatchAcks = cfg
	}
}

// WithBatchUnacks configures batch unacknowledgments.
func WithBatchUnacks(cfg BatchConfig) Option {
	return func(o *Options) {
		o.BatchUnacks = cfg
	}
}
