/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package redis

import (
	"context"
	"fmt"
)

// LoadListMembers wraps LRange with standard error handling.
// Returns an empty slice if the list has no members.
//
// Example:
//
//	members, err := redis.LoadListMembers(ctx, key, "processing purge jobs")
func LoadListMembers(ctx context.Context, key string, entity string) ([]string, error) {
	members, err := Client().LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", entity, err)
	}
	return members, nil
}
