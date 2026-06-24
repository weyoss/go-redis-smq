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

import "time"

// Config controls the Redis client and its internal connection pool.
// Zero values use go-redis defaults.
type Config struct {
	// Addr is the Redis server address (required).
	Addr string

	// Password is the Redis password.
	Password string

	// DB is the Redis database number.
	DB int

	// PoolSize is the maximum number of connections (default: 10 * GOMAXPROCS).
	PoolSize int

	// MinIdleConns is the minimum number of idle connections (default: 0).
	MinIdleConns int

	// ConnMaxIdleTime is the maximum idle time for a connection (default: 30m).
	ConnMaxIdleTime time.Duration

	// ConnMaxLifetime is the maximum age of a connection (default: 0 = forever).
	ConnMaxLifetime time.Duration

	// PoolTimeout is the amount of time to wait for a connection when pool is exhausted.
	PoolTimeout time.Duration
}
