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
//
// This file contains the Redis client singleton — the single point of
// configuration and connection management for the entire application.
package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
)

// Hook is a callback interface for observing or intercepting Redis commands.
// Use it for logging, metrics, or tracing.
type Hook interface {
	redis.Hook
}

// Singleton state
var (
	client    atomic.Pointer[redis.Client]
	scriptMgr atomic.Pointer[scripts.ScriptManager]
	mu        sync.Mutex
)

// Init creates the shared Redis client, verifies connectivity, and loads
// all Lua scripts into Redis. Safe to call multiple times; returns immediately
// if already initialized. If a previous call failed, retries initialization.
// Panics if addr is empty.
func Init(ctx context.Context, cfg Config) error {
	if cfg.Addr == "" {
		panic("redis: addr is required")
	}

	// Fast path: already successfully initialized
	if c := client.Load(); c != nil {
		return nil
	}

	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring lock
	if c := client.Load(); c != nil {
		return nil
	}

	c, err := dial(ctx, cfg)
	if err != nil {
		return err
	}

	sm := scripts.NewScriptManager(c)
	if err := sm.RegisterAll(ctx); err != nil {
		_ = c.Close()
		return fmt.Errorf("redis: %w", err)
	}

	client.Store(c)
	scriptMgr.Store(sm)
	return nil
}

// Client returns the shared Redis client.
// Panics if Init has not been called.
func Client() *redis.Client {
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

// Close shuts down the shared Redis client and releases all pool connections.
// After Close, Client() and Eval() will panic. Safe to call multiple times.
// After Close, Init() can be called again to reinitialize.
func Close() {
	mu.Lock()
	defer mu.Unlock()

	scriptMgr.Swap(nil)
	c := client.Swap(nil)
	if c != nil {
		_ = c.Close()
	}
}

// Stats returns connection pool statistics for health checks and monitoring.
func Stats() *redis.PoolStats {
	return Client().PoolStats()
}

// AddHook attaches a Hook to the shared client for observability.
// Must be called after Init.
func AddHook(hook Hook) {
	Client().AddHook(hook)
}

// dial creates a new Redis client and verifies connectivity.
func dial(ctx context.Context, cfg Config) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		PoolTimeout:     cfg.PoolTimeout,
	})

	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: connect failed: %w", err)
	}
	return c, nil
}

func Conn() *redis.Conn {
	return Client().Conn()
}
