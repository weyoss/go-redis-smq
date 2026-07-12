/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package config

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	internalConfig "github.com/weyoss/go-redis-smq/internal/config"
	internalConfigEvents "github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/errs"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	"github.com/weyoss/go-redis-smq/pkg/config/cfg"
	"github.com/weyoss/go-redis-smq/pkg/config/events"
)

var (
	instance atomic.Pointer[cfg.Config]
	mu       sync.Mutex
	codec    = internalConfig.NewCodec()
)

// Init initializes the configuration singleton.
// Safe to call multiple times; subsequent calls are no-ops.
func Init(ctx context.Context) error {
	if instance.Load() != nil {
		return nil
	}

	mu.Lock()
	defer mu.Unlock()

	if instance.Load() != nil {
		return nil
	}

	c := loadOrDefault(ctx)
	if c == nil {
		return fmt.Errorf("config: failed to load configuration")
	}

	instance.Store(c)
	subscribeToUpdates()
	return nil
}

// Get returns the current configuration.
// Panics if Init has not been called.
func Get() *cfg.Config {
	c := instance.Load()
	if c == nil {
		panic("config: not initialized — call config.Init() first")
	}
	return c
}

// Close releases the configuration singleton.
func Close() {
	mu.Lock()
	defer mu.Unlock()
	instance.Swap(nil)
}

// Save persists the configuration and publishes an update event.
// Use config.Get() to obtain the current config, modify fields as needed,
// then pass the result to Save. The config replaces the entire stored config.
func Save(ctx context.Context, c *cfg.Config) (int, error) {
	mu.Lock()
	defer mu.Unlock()

	current := instance.Load()
	if current == nil {
		return 0, cfg.ErrNotInitialized
	}

	version, err := save(ctx, c, current.Version)
	if err != nil {
		return 0, err
	}

	c.Version = version
	instance.Store(c)

	internalConfigEvents.PublishUpdated(ctx, internalConfigEvents.UpdatedPayload{
		Config:  c,
		Version: version,
	})

	return version, nil
}

// Reset restores the configuration to factory defaults.
func Reset(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()

	current := instance.Load()
	if current == nil {
		return cfg.ErrNotInitialized
	}

	defaults := cfg.DefaultConfig()
	version, err := save(ctx, defaults, current.Version)
	if err != nil {
		return err
	}

	defaults.Version = version
	instance.Store(defaults)

	internalConfigEvents.PublishUpdated(ctx, internalConfigEvents.UpdatedPayload{
		Config:  defaults,
		Version: version,
	})

	return nil
}

// Reload reloads the configuration from Redis.
func Reload(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()

	if instance.Load() == nil {
		return cfg.ErrNotInitialized
	}

	key := keys.System{}.Config()
	hash, err := redisClient.LoadHash(ctx, key, "config")
	if err != nil {
		defaults := cfg.DefaultConfig()
		version, saveErr := save(ctx, defaults, 0)
		if saveErr != nil {
			return fmt.Errorf("reload config: save defaults: %w", saveErr)
		}
		defaults.Version = version
		instance.Store(defaults)
		return nil
	}

	c, err := codec.DecodeHash(ctx, hash)
	if err != nil {
		return fmt.Errorf("reload config: decode: %w", err)
	}

	instance.Store(c)
	return nil
}

func loadOrDefault(ctx context.Context) *cfg.Config {
	key := keys.System{}.Config()

	hash, err := redisClient.LoadHash(ctx, key, "config")
	if err != nil {
		return saveDefaults(ctx, 0)
	}

	c, err := codec.DecodeHash(ctx, hash)
	if err != nil {
		currentVersion := 0
		if v, ok := hash[internalConfig.ConfigFieldVersion]; ok {
			fmt.Sscanf(v, "%d", &currentVersion)
		}
		return saveDefaults(ctx, currentVersion)
	}

	return c
}

func saveDefaults(ctx context.Context, expectedVersion int) *cfg.Config {
	defaults := cfg.DefaultConfig()
	version, err := save(ctx, defaults, expectedVersion)
	if err != nil {
		fmt.Printf("config: failed to save defaults: %v\n", err)
	}
	defaults.Version = version
	return defaults
}

func subscribeToUpdates() {
	events.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		mu.Lock()
		defer mu.Unlock()

		c := instance.Load()
		if c == nil {
			return
		}

		if p.Version > c.Version {
			c.Version = p.Config.Version
			c.Namespace = p.Config.Namespace
			c.Logger = p.Config.Logger
			c.MessageAudit = p.Config.MessageAudit
		}
	})
}

func save(ctx context.Context, c *cfg.Config, currentVersion int) (int, error) {
	key := keys.System{}.Config()

	configData, err := json.Marshal(c)
	if err != nil {
		return 0, fmt.Errorf("marshal config: %w", err)
	}

	reply, err := redisClient.Eval(ctx, scripts.SaveConfig,
		[]string{key},
		[]interface{}{
			internalConfig.ConfigFieldVersion,
			internalConfig.ConfigFieldData,
			currentVersion,
			string(configData),
		},
	)
	if err != nil {
		return 0, fmt.Errorf("save config: %w", err)
	}

	if replyStr, ok := reply.(string); ok {
		if replyStr == "VERSION_MISMATCH" {
			return 0, cfg.ErrVersionMismatch
		}
		return 0, fmt.Errorf("%w: %s", errs.ErrUnexpectedScriptReply, replyStr)
	}

	version, err := redisClient.Int64(reply)
	if err != nil {
		return 0, err
	}

	return int(version), nil
}
