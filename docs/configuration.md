# Configuration

RedisSMQ configuration controls system‑wide settings. Configuration is stored in Redis and shared across all connected instances. The public API exposes configuration types and a `Manager` interface; the concrete manager is provided by `redissmq.NewConfigManager()`.

## Obtain the Manager

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
)

ctx := context.Background()
cfgManager := redissmq.NewConfigManager()
```

The manager is a singleton owned by RedisSMQ. It is initialised automatically during `redissmq.Init()`.

## Read Configuration

```go
cfg := cfgManager.Get()
fmt.Println("Namespace:", cfg.Namespace)
fmt.Println("Logger enabled:", cfg.Logger.Enabled)
```

## Update Configuration

Always start from `cfgManager.Get()`, modify fields, and save:

```go
cfg := cfgManager.Get()
cfg.Logger.Enabled = true
cfg.Logger.Options.LogLevel = 0 // DEBUG
cfg.MessageAudit.AcknowledgedMessages.Enabled = true

version, err := cfgManager.Save(ctx, cfg)
if err != nil {
    log.Fatal(err)
}
fmt.Println("New version:", version)
```

## Reset to Defaults

```go
if err := cfgManager.Reset(ctx); err != nil {
    log.Fatal(err)
}
```

## Reload from Redis

```go
if err := cfgManager.Reload(ctx); err != nil {
    log.Fatal(err)
}
```

## Configuration Snapshot

In addition to the manager, the public `config` package maintains a thread‑safe snapshot of the current configuration. It is updated automatically when the configuration changes, and can be used by public packages that need read‑only access without importing internal code.

```go
import "github.com/weyoss/go-redis-smq/pkg/config"

// Get the latest snapshot
cfg := config.Get()

// Get the default namespace used by queue/exchange constructors
ns := config.DefaultNamespace()
```

## Configuration Options

```go
type Config struct {
    Namespace    string
    Logger       LoggerConfig
    MessageAudit MessageAudit
}

type LoggerConfig struct {
    Enabled bool
    Options LoggerOptionsConfig
}

type LoggerOptionsConfig struct {
    IncludeTimestamp bool
    Colorize         bool
    LogLevel         int // 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR
}

type MessageAudit struct {
    AcknowledgedMessages     AuditMessagesConfig
    DeadLetteredMessages     AuditMessagesConfig
    UnacknowledgementHistory AuditHistoryConfig
}

type AuditMessagesConfig struct {
    Enabled   bool
    QueueSize int // 0 = unlimited
    Expire    int // seconds, 0 = never
}

type AuditHistoryConfig struct {
    Enabled bool
    MaxSize int // 0 = unlimited
}
```

## Default Configuration

```go
defaults := config.DefaultConfig()
// Namespace: "default"
// Logger: disabled
// Message audit: disabled
```

## Cross‑Instance Sync

Configuration changes are published as internal events. All connected instances receive updates automatically via the system event bus. Version checking prevents conflicting updates.

If another instance modifies the configuration between your `Get()` and `Save()`, `Save` returns `config.ErrVersionMismatch`. Re‑read and retry:

```go
cfg := cfgManager.Get()
cfg.Logger.Enabled = true
_, err := cfgManager.Save(ctx, cfg)
if errors.Is(err, config.ErrVersionMismatch) {
    // re-read and retry
    cfg = cfgManager.Get()
    cfg.Logger.Enabled = true
    _, err = cfgManager.Save(ctx, cfg)
}
```

## Related

- [Configuration Concepts](https://github.com/weyoss/redis-smq-docs) — How configuration works
- [Message Browsing](message-browsing.md) — Audit must be enabled for acknowledged/dead-lettered browsing
