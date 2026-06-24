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

	"github.com/redis/go-redis/v9"
)

const defaultMaxAttempts = 3

// WithTransaction executes a function within a Redis transaction with retries.
// Automatically retries on optimistic lock failures (TxFailedErr).
//
// The function receives a *redis.Tx that can be used to read watched keys
// and build a pipeline of commands to execute atomically.
//
// Example:
//
//	err := redis.WithTransaction(ctx, watchKeys, 3, func(tx *redis.Tx) error {
//	    // Read values under WATCH
//	    val, err := tx.Get(ctx, key).Result()
//	    if err != nil {
//	        return err
//	    }
//
//	    // Execute writes atomically
//	    _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
//	        pipe.Set(ctx, key, newVal, 0)
//	        return nil
//	    })
//	    return err
//	})
func WithTransaction(ctx context.Context, watchKeys []string, maxAttempts int, fn func(*redis.Tx) error) error {
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := Client().Watch(ctx, fn, watchKeys...)
		if err == nil {
			return nil
		}
		if err != redis.TxFailedErr {
			return fmt.Errorf("transaction failed: %w", err)
		}
	}

	return fmt.Errorf("transaction failed after %d attempts", maxAttempts)
}
