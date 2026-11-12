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
//
// On subscription failure (internalConfigEvents.SubscribeUpdated returning
// an error), Init rolls back the loaded config and returns the error.
func (m *Manager) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.instance != nil {
		return nil
	}
	if err := m.loadOrCreateLocked(ctx); err != nil {
		return err
	}
	if err := m.subscribeToUpdatesLocked(); err != nil {
		// Roll back the loaded config so a retry starts clean. The Redis
		// state (config hash, and defaults if just created) is left in
		// place — loadOrCreateLocked is idempotent and will re-read it on
		// retry. Only the in-memory singleton and its public snapshot are
		// cleared, matching Close's post-state.
		m.instance = nil
		m.epoch = ""
		pubconfig.Set(nil)
		return fmt.Errorf("config: subscribe to updates: %w", err)
	}
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

// Close releases the configuration singleton and clears the public snapshot.
//
// After Close, pubconfig.Get() returns nil until the next Init.
//
// Idempotent: safe to call multiple times. A subsequent Init restores the
// snapshot from Redis (or creates fresh defaults).
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sub != nil {
		m.sub.Unsubscribe()
		m.sub = nil
	}
	m.instance = nil
	m.epoch = ""
	pubconfig.Set(nil)
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

// subscribeToUpdatesLocked subscribes to configuration.updated events on the
// system bus. Returns an error if subscription fails — the caller (Init)
// propagates it rather than silently running with peer sync disabled.
//
// Hybrid dispatch based on the presence of the epoch argument:
//
//   - 3-arg event (p.Epoch != "") came from a Go publisher. Use the
//     pre-M1 epoch-based staleness detection: if the epoch differs from
//     local, Reload from Redis; otherwise apply the event's config if
//     its version is newer. This preserves the no-op fast path for
//     rapid-save self-echoes and correctly rejects stale events from
//     previous Close/Init cycles.
//
//   - 2-arg event (p.Epoch == "") came from a TypeScript publisher. There
//     is no epoch to compare — TS never generates one — so fall back to
//     version-based detection. Any version difference (higher or lower)
//     triggers a Reload from Redis. Same-version events are no-ops
//     (duplicate delivery).
//
//     Reload-on-any-difference is correct for TS-originated events
//     because (a) TS saves are rare, so the HGETALL cost is negligible,
//     (b) there is no self-echo case (Go doesn't emit 2-arg events), and
//     (c) it can't apply a stale payload — Redis is the single source
//     of truth for what m.instance gets set to.
//
//     Version-only comparison with apply-on-higher
//     is NOT used here, because it can corrupt state when a stale 2-arg
//     event with a higher version arrives from a previous TS session.
//     Reload-on-difference is the safe fallback without an epoch.
//
// The `go func() { m.Reload(...) }()` indirection is untracked. It is
// safe: Reload takes m.mu (serializes with Close); Close nils m.instance
// before redissmq.Shutdown proceeds to internalredis.Close. So the
// goroutine either sees m.instance == nil (early-returns) or runs its
// Redis ops before internalredis.Close fires.
func (m *Manager) subscribeToUpdatesLocked() error {
	sub, err := internalConfigEvents.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		m.mu.Lock()
		defer m.mu.Unlock()

		if m.instance == nil {
			return
		}

		if p.Epoch != "" {
			// Go-originated event — epoch-based dispatch.
			if p.Epoch != m.epoch {
				go func() { _ = m.Reload(context.Background()) }()
				return
			}
			if p.Version > m.instance.Version {
				m.setCurrentConfigLocked(p.Config, p.Version)
			}
			return
		}

		// TS-originated event — no epoch, fall back to reload-on-difference.
		if p.Version != m.instance.Version {
			go func() { _ = m.Reload(context.Background()) }()
		}
	})
	if err != nil {
		return fmt.Errorf("config: subscribe to updates: %w", err)
	}
	m.sub = sub
	return nil
}
