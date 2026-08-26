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
	"github.com/weyoss/go-redis-smq/pkg/config"
)

// PublishUpdated publishes a configuration.updated event to the system bus.
//
// This event is internal-only and is used to synchronise configuration
// changes across connected instances.
func PublishUpdated(ctx context.Context, config *config.Config, version int) {
	eventmultiplexer.Publish(ctx, EventUpdated, config, version)
}
