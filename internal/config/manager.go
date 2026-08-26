package config

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	internalConfigEvents "github.com/weyoss/go-redis-smq/internal/config/events"
	"github.com/weyoss/go-redis-smq/internal/errs"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	pubconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// Manager is the concrete implementation of pubconfig.Manager.
//
// It manages the configuration singleton, including loading, saving,
// resetting, reloading, and subscribing to cross‑instance updates.
type Manager struct {
	mu       sync.Mutex
	instance *pubconfig.Config
	codec    *Codec
	sub      *eventbus.Subscription
}

// defaultManager is the package‑level singleton used by internal components.
var defaultManager = NewManager()

// DefaultManager returns the package‑level singleton configuration manager.
func DefaultManager() *Manager {
	return defaultManager
}

// NewManager creates a new configuration manager.
func NewManager() *Manager {
	return &Manager{
		codec: NewCodec(),
	}
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

	c := m.loadOrDefault(ctx)
	if c == nil {
		return fmt.Errorf("config: failed to load configuration")
	}

	m.instance = c
	pubconfig.Set(c)

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

	version, err := m.saveLocked(ctx, c, m.instance.Version)
	if err != nil {
		return 0, err
	}

	c.Version = version
	m.instance = c
	pubconfig.Set(c)

	internalConfigEvents.PublishUpdated(ctx, c, version)

	return version, nil
}

// Reset restores the configuration to factory defaults.
func (m *Manager) Reset(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.instance == nil {
		return pubconfig.ErrNotInitialized
	}

	defaults := pubconfig.DefaultConfig()
	version, err := m.saveLocked(ctx, defaults, m.instance.Version)
	if err != nil {
		return err
	}

	defaults.Version = version
	m.instance = defaults
	pubconfig.Set(defaults)

	internalConfigEvents.PublishUpdated(ctx, defaults, version)

	return nil
}

// Reload reloads the configuration from Redis.
func (m *Manager) Reload(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.instance == nil {
		return pubconfig.ErrNotInitialized
	}

	key := keys.System{}.Config()
	hash, err := redisClient.LoadHash(ctx, key, "config")
	if err != nil {
		defaults := pubconfig.DefaultConfig()
		version, saveErr := m.saveLocked(ctx, defaults, 0)
		if saveErr != nil {
			return fmt.Errorf("reload config: save defaults: %w", saveErr)
		}
		defaults.Version = version
		m.instance = defaults
		pubconfig.Set(defaults)
		return nil
	}

	c, err := m.codec.DecodeHash(ctx, hash)
	if err != nil {
		return fmt.Errorf("reload config: decode: %w", err)
	}

	m.instance = c
	pubconfig.Set(c)

	return nil
}

// Close releases the configuration singleton and unsubscribes from updates.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sub != nil {
		m.sub.Unsubscribe()
		m.sub = nil
	}
	m.instance = nil
}

// --- private helpers ---

func (m *Manager) loadOrDefault(ctx context.Context) *pubconfig.Config {
	key := keys.System{}.Config()

	hash, err := redisClient.LoadHash(ctx, key, "config")
	if err != nil {
		return m.saveDefaultsLocked(ctx, 0)
	}

	c, err := m.codec.DecodeHash(ctx, hash)
	if err != nil {
		currentVersion := 0
		if v, ok := hash[ConfigFieldVersion]; ok {
			fmt.Sscanf(v, "%d", &currentVersion)
		}
		return m.saveDefaultsLocked(ctx, currentVersion)
	}

	return c
}

func (m *Manager) saveDefaultsLocked(ctx context.Context, expectedVersion int) *pubconfig.Config {
	defaults := pubconfig.DefaultConfig()
	version, err := m.saveLocked(ctx, defaults, expectedVersion)
	if err != nil {
		fmt.Printf("config: failed to save defaults: %v\n", err)
		return nil
	}
	defaults.Version = version
	return defaults
}

func (m *Manager) saveLocked(ctx context.Context, c *pubconfig.Config, currentVersion int) (int, error) {
	key := keys.System{}.Config()

	configData, err := json.Marshal(c)
	if err != nil {
		return 0, fmt.Errorf("marshal config: %w", err)
	}

	reply, err := redisClient.Eval(ctx, scripts.SaveConfig,
		[]string{key},
		[]interface{}{
			ConfigFieldVersion,
			ConfigFieldData,
			currentVersion,
			string(configData),
		},
	)
	if err != nil {
		return 0, fmt.Errorf("save config: %w", err)
	}

	if replyStr, ok := reply.(string); ok {
		if replyStr == "VERSION_MISMATCH" {
			return 0, pubconfig.ErrVersionMismatch
		}
		return 0, fmt.Errorf("%w: %s", errs.ErrUnexpectedScriptReply, replyStr)
	}

	version, err := redisClient.Int64(reply)
	if err != nil {
		return 0, err
	}

	return int(version), nil
}

func (m *Manager) subscribeToUpdatesLocked() {
	sub, err := internalConfigEvents.SubscribeUpdated(func(p internalConfigEvents.UpdatedPayload) {
		m.mu.Lock()
		defer m.mu.Unlock()

		if m.instance == nil {
			return
		}

		if p.Version > m.instance.Version {
			m.instance = p.Config
			pubconfig.Set(p.Config)
		}
	})
	if err != nil {
		fmt.Printf("config: failed to subscribe to configuration updates: %v\n", err)
		return
	}
	m.sub = sub
}
