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

// LoadHash wraps HGetAll with standard error handling.
// Returns an error if the hash is empty or the Redis operation fails.
//
// Example:
//
//	hash, err := redis.LoadHash(ctx, key, "queue properties")
func LoadHash(ctx context.Context, key string, entity string) (map[string]string, error) {
	hash, err := Client().HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", entity, err)
	}
	if len(hash) == 0 {
		return nil, fmt.Errorf("%s not found", entity)
	}
	return hash, nil
}

// LoadHashField reads a single field from a Redis hash.
// Returns an error if the field doesn't exist or the Redis operation fails.
//
// Example:
//
//	value, err := redis.LoadHashField(ctx, key, "status", "message status")
func LoadHashField(ctx context.Context, key, field, entity string) (string, error) {
	value, err := Client().HGet(ctx, key, field).Result()
	if err != nil {
		return "", fmt.Errorf("load %s field %s: %w", entity, field, err)
	}
	return value, nil
}
