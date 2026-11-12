/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package events

import (
	"context"

	"github.com/weyoss/go-redis-smq/internal/eventmultiplexer"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

var log = logger.New("config", "events")

// PublishUpdated publishes a configuration.updated event on the system bus.
//
// This event is internal-only and is used to synchronise configuration
// changes across connected instances.
//
// The three-argument form (config, version, epoch) is the Go publisher's
// native wire format. Go subscribers consume all three (epoch is used
// for staleness detection — see subscribe.go). TypeScript subscribers
// define their handler with two parameters (config, version); JavaScript
// handler dispatch silently ignores the third argument, so a 3-arg
// publish is accepted by both Go and TypeScript subscribers.
//
// TypeScript publishers emit only two args. Go subscribers detect this
// by the absence of the epoch argument and fall back to version-only
// comparison — see subscribe.go.
func PublishUpdated(ctx context.Context, cfg *pubconfig.Config, version int, epoch string) {
	if err := eventmultiplexer.Publish(ctx, EventUpdated, cfg, version, epoch); err != nil {
		log.Error("failed to publish event",
			"event", EventUpdated,
			"error", err,
		)
	}
}
