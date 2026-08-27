/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package redis provides the Redis client singleton and shared utilities
// for hash, set, and transaction operations.
package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	goredis "github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
)

// Hook is a callback interface for observing or intercepting Redis commands.
type Hook interface {
	goredis.Hook
}

var (
	client    atomic.Pointer[goredis.Client]
	scriptMgr atomic.Pointer[scripts.ScriptManager]
	mu        sync.Mutex
)

// Init sets the shared Redis client, verifies connectivity, and loads all
// Lua scripts into Redis.
//
// The provided client must be a *goredis.Client. Cluster and ring clients are
// rejected because RedisSMQ uses multi-key Lua scripts that require all keys
// to reside on a single Redis node.
//
// Safe to call multiple times; returns immediately if already initialized.
// If a previous call failed, retries initialization.
func Init(ctx context.Context, incoming goredis.UniversalClient) error {
	c, ok := incoming.(*goredis.Client)
	if !ok {
		return fmt.Errorf("redis: only *redis.Client is supported; cluster and ring clients are not allowed")
	}

	// Fast path: already successfully initialized
	if current := client.Load(); current != nil {
		return nil
	}

	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring lock
	if current := client.Load(); current != nil {
		return nil
	}

	// Verify connectivity
	if err := c.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: connect failed: %w", err)
	}

	sm := scripts.NewScriptManager(c)
	if err := sm.RegisterAll(ctx); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	client.Store(c)
	scriptMgr.Store(sm)
	return nil
}

// Client returns the shared Redis client.
// Panics if Init has not been called.
func Client() *goredis.Client {
	c := client.Load()
	if c == nil {
		panic("redis: not initialized — call redis.Init() during bootstrap")
	}
	return c
}

// Eval executes a pre-registered Lua script by ID.
// Uses the cached SHA for efficiency; falls back to EVAL on NOSCRIPT errors.
func Eval(ctx context.Context, id scripts.ID, keys []string, args ...interface{}) (interface{}, error) {
	sm := scriptMgr.Load()
	if sm == nil {
		panic("redis: not initialized — call redis.Init() during bootstrap")
	}
	return sm.Eval(ctx, id, keys, args...)
}

// Close clears the shared Redis client and script manager references.
//
// It does NOT close the Redis client itself, because the client is now
// owned by the caller (the user who provided it to Init). After Close,
// Client() and Eval() will panic. Safe to call multiple times.
// After Close, Init() can be called again with a new client.
func Close() {
	mu.Lock()
	defer mu.Unlock()

	scriptMgr.Swap(nil)
	client.Swap(nil)
}

// Stats returns connection pool statistics for health checks and monitoring.
func Stats() *goredis.PoolStats {
	return Client().PoolStats()
}

// AddHook attaches a Hook to the shared client for observability.
// Must be called after Init.
func AddHook(hook Hook) {
	Client().AddHook(hook)
}

// Conn returns a new connection from the shared client.
func Conn() *goredis.Conn {
	return Client().Conn()
}
