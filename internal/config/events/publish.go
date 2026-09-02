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

// PublishUpdated publishes a configuration.updated event to the system bus.
//
// This event is internal-only and is used to synchronise configuration
// changes across connected instances.
//
// The epoch uniquely identifies the generation of the configuration record.
// Subscribers compare it with their local epoch to determine whether the
// event is relevant or stale.
func PublishUpdated(ctx context.Context, cfg *pubconfig.Config, version int, epoch string) {
	if err := eventmultiplexer.Publish(ctx, EventUpdated, cfg, version, epoch); err != nil {
		log.Error("failed to publish event",
			"event", EventUpdated,
			"error", err,
		)
	}
}
