/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package producer

import (
	"context"

	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
)

// Producer is the public interface implemented by RedisSMQ producers.
type Producer interface {
	// Run starts the producer and prepares it for publishing.
	Run(ctx context.Context) error

	// Shutdown gracefully stops the producer.
	Shutdown(ctx context.Context)

	// IsRunning reports whether the producer is currently running.
	IsRunning() bool

	// ID returns the unique identifier of the producer.
	ID() string

	// Produce publishes a message to its configured destination.
	Produce(ctx context.Context, m *publicmessage.ProducibleMessage) ([]string, error)
}
