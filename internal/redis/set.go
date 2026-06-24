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

// LoadSetMembers wraps SMembers with standard error handling.
// Returns an empty slice if the set has no members.
//
// Example:
//
//	members, err := redis.LoadSetMembers(ctx, key, "all exchanges")
func LoadSetMembers(ctx context.Context, key string, entity string) ([]string, error) {
	members, err := Client().SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("load %s members: %w", entity, err)
	}
	if len(members) == 0 {
		return []string{}, nil
	}
	return members, nil
}

// SetContains checks if a value exists in a Redis set.
//
// Example:
//
//	exists, err := redis.SetContains(ctx, key, value, "exchange index")
func SetContains(ctx context.Context, key, value, entity string) (bool, error) {
	exists, err := Client().SIsMember(ctx, key, value).Result()
	if err != nil {
		return false, fmt.Errorf("check %s membership: %w", entity, err)
	}
	return exists, nil
}
