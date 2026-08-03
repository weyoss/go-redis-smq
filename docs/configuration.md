# Configuration

RedisSMQ configuration controls system-wide settings. Configuration is stored in Redis and shared across all connected
instances.

## Read Configuration

```go
import "github.com/weyoss/go-redis-smq/pkg/config"

cfg := config.Get()
fmt.Println("Namespace:", cfg.Namespace)
fmt.Println("Logger enabled:", cfg.Logger.Enabled)
```

## Update Configuration

```go
cfg := config.Get()
cfg.Logger.Enabled = true
cfg.Logger.Options.LogLevel = 0 // DEBUG
cfg.MessageAudit.AcknowledgedMessages.Enabled = true

version, err := config.Save(ctx, cfg)
fmt.Println("New version:", version)
```

Always use `config.Get()` as the starting point. `config.Save()` replaces the entire configuration — passing a partial
struct will zero out unset fields.

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
defaults := cfg.DefaultConfig()
// Namespace: "default"
// Logger: disabled
// Message audit: disabled
```

## Cross-Instance Sync

Configuration changes are published as events. All connected instances receive updates automatically via the event bus.
Version checking prevents conflicting updates.

## Related

- [Configuration Concepts](../../../docs/configuration.md) — How configuration works
- [Message Browsing](message-browsing.md) — Audit must be enabled for acknowledged/dead-lettered browsing