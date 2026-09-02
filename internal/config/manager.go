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

	"github.com/google/uuid"

	internalConfigEvents "github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// Manager is the concrete implementation of pubconfig.Manager.
type Manager struct {
	mu       sync.Mutex
	instance *pubconfig.Config
	codec    *Codec
	sub      *eventbus.Subscription
	epoch    string
}

// defaultManager is the package‑level singleton used by internal components.
var defaultManager = NewManager()

// DefaultManager returns the package‑level singleton configuration manager.
func DefaultManager() *Manager {
	return defaultManager
}

// NewManager creates a new configuration manager.
func NewManager() *Manager {
	return &Manager{codec: NewCodec()}
}

// Init loads or creates the configuration singleton.
// It is safe to call multiple times; subsequent calls are no‑ops.
func Init(ctx context.Context) error {
	return defaultManager.Init(ctx)
}

// Get returns the current configuration.
// It panics if Init has not been called.
func Get() *pubconfig.Config {
	return defaultManager.Get()
}

// Save persists the given configuration and returns the new version.
func Save(ctx context.Context, cfg *pubconfig.Config) (int, error) {
	return defaultManager.Save(ctx, cfg)
}

// Reset restores the configuration to factory defaults.
func Reset(ctx context.Context) error {
	return defaultManager.Reset(ctx)
}

// Reload reloads the configuration from Redis.
func Reload(ctx context.Context) error {
	return defaultManager.Reload(ctx)
}

// Close releases the configuration singleton.
func Close() {
	defaultManager.Close()
}

// Init loads or creates the configuration singleton.
func (m *Manager) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance != nil {
		return nil
	}
	if err := m.loadOrCreateLocked(ctx); err != nil {
		return err
	}
	m.subscribeToUpdatesLocked()
	return nil
}

// Get returns the current configuration.
func (m *Manager) Get() *pubconfig.Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance == nil {
		panic("config: not initialized — call Init first")
	}
	return m.instance
}

// Save persists the given configuration and publishes an update event.
func (m *Manager) Save(ctx context.Context, c *pubconfig.Config) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance == nil {
		return 0, pubconfig.ErrNotInitialized
	}
	return m.persistAndPublishLocked(ctx, c)
}

// Reset restores the configuration to factory defaults.
func (m *Manager) Reset(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance == nil {
		return pubconfig.ErrNotInitialized
	}
	_, err := m.persistAndPublishLocked(ctx, pubconfig.DefaultConfig())
	return err
}

// Reload reloads the configuration from Redis.
func (m *Manager) Reload(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance == nil {
		return pubconfig.ErrNotInitialized
	}
	return m.loadOrCreateLocked(ctx)
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sub != nil {
		m.sub.Unsubscribe()
		m.sub = nil
	}
	m.instance = nil
}

// persistAndPublishLocked persists the given config, updates in‑memory state,
// and publishes an update event. It assumes the lock is held.
func (m *Manager) persistAndPublishLocked(ctx context.Context, c *pubconfig.Config) (int, error) {
	version, err := m.persistLocked(ctx, c, m.instance.Version)
	if err != nil {
		return 0, err
	}
	m.setCurrentConfigLocked(c, version)
	internalConfigEvents.PublishUpdated(ctx, c, version, m.epoch)
	return version, nil
}

// setCurrentConfigLocked updates the in‑memory instance and public snapshot.
func (m *Manager) setCurrentConfigLocked(c *pubconfig.Config, version int) {
	c.Version = version
	m.instance = c
	pubconfig.Set(c)
}

// persistLocked saves the config and ensures the epoch exists in Redis.
func (m *Manager) persistLocked(ctx context.Context, c *pubconfig.Config, expectedVersion int) (int, error) {
	if m.epoch == "" {
		m.epoch = uuid.New().String()
	}
	version, err := m.saveLocked(ctx, c, expectedVersion)
	if err != nil {
		return 0, err
	}
	key := keys.System{}.Config()
	if err := redisClient.Client().HSet(ctx, key, "epoch", m.epoch).Err(); err != nil {
		return 0, fmt.Errorf("persist config epoch: %w", err)
	}
	return version, nil
}

// saveLocked uses the version‑checked Lua script to save the config.
func (m *Manager) saveLocked(ctx context.Context, c *pubconfig.Config, currentVersion int) (int, error) {
	key := keys.System{}.Config()
	configData, err := json.Marshal(c)
	if err != nil {
		return 0, fmt.Errorf("marshal config: %w", err)
	}
	reply, err := redisClient.Eval(ctx, scripts.SaveConfig,
		[]string{key},
		[]interface{}{FieldVersion, FieldData, currentVersion, string(configData)},
	)
	if err != nil {
		return 0, fmt.Errorf("save config: %w", err)
	}
	if replyStr, ok := reply.(string); ok {
		if replyStr == "VERSION_MISMATCH" {
			return 0, pubconfig.ErrVersionMismatch
		}
		return 0, fmt.Errorf("%w: %s", pubconfig.ErrUnexpectedScriptReply, replyStr)
	}
	version, err := redisClient.Int64(reply)
	if err != nil {
		return 0, err
	}
	return int(version), nil
}

// loadOrCreateLocked loads config and epoch from Redis, or creates defaults
// if missing/corrupted. Assumes the lock is held.
func (m *Manager) loadOrCreateLocked(ctx context.Context) error {
	key := keys.System{}.Config()
	hash, err := redisClient.LoadHash(ctx, key, "config")
	if err == nil {
		c, decodeErr := m.codec.Decode(ctx, hash)
		if decodeErr == nil {
			epoch := hash["epoch"]
			if epoch == "" {
				m.epoch = uuid.New().String()
				if setErr := redisClient.Client().HSet(ctx, key, "epoch", m.epoch).Err(); setErr != nil {
					return fmt.Errorf("set epoch: %w", setErr)
				}
			} else {
				m.epoch = epoch
			}
			m.setCurrentConfigLocked(c, c.Version)
			return nil
		}
	}
	// Load failed or config missing/corrupted -> create defaults
	return m.createDefaultsLocked(ctx)
}

// createDefaultsLocked creates and persists default config with a new epoch.
func (m *Manager) createDefaultsLocked(ctx context.Context) error {
	m.epoch = uuid.New().String()
	defaults := pubconfig.DefaultConfig()
	version, err := m.persistLocked(ctx, defaults, 0)
	if err != nil {
		return fmt.Errorf("create defaults: %w", err)
	}
	m.setCurrentConfigLocked(defaults, version)
	return nil
}

// subscribeToUpdatesLocked subscribes to configuration.updated events.
// It filters events by epoch and triggers a reload on mismatch.
func (m *Manager) subscribeToUpdatesLocked() {
	sub, err := internalConfigEvents.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		m.mu.Lock()
		defer m.mu.Unlock()

		if m.instance == nil {
			return
		}
		if p.Epoch != m.epoch {
			go func() { _ = m.Reload(context.Background()) }()
			return
		}
		if p.Version > m.instance.Version {
			m.setCurrentConfigLocked(p.Config, p.Version)
		}
	})
	if err != nil {
		fmt.Printf("config: failed to subscribe to configuration updates: %v\n", err)
		return
	}
	m.sub = sub
}
