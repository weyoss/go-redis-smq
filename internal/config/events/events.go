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
	"github.com/weyoss/go-redis-smq/pkg/config/cfg"
)

const EventUpdated = "configuration.updated"

type UpdatedPayload struct {
	Config  *cfg.Config `json:"config"`
	Version int         `json:"version"`
}
