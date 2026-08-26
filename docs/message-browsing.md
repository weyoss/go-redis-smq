# Message Browsing

Browse messages in a queue by category. Paginate through results.

## Browse Messages

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

qm := redissmq.NewQueueManager()
```

### Published Messages

All messages in the queue:

```go
result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowsePublished,
    Offset: 0,
    Count:  100,
})
```

### Pending Messages

Messages waiting to be consumed:

```go
result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowsePending,
})
```

For FIFO/LIFO queues, ordered by arrival. For priority queues, ordered by priority.

### Scheduled Messages

Messages waiting for future delivery:

```go
result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowseScheduled,
})
```

### Acknowledged Messages

Successfully processed messages. Requires audit enabled:

```go
result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowseAcknowledged,
})
```

Returns an error if `MessageAudit.AcknowledgedMessages` is not enabled.

### Dead-Lettered Messages

Failed messages. Requires audit enabled:

```go
result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowseDeadLettered,
})
```

Returns an error if `MessageAudit.DeadLetteredMessages` is not enabled.

## Enable Message Audit

Before browsing acknowledged or dead-lettered messages, enable the corresponding audit category via the configuration manager:

```go
import (
    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/config"
)

cfgManager := redissmq.NewConfigManager()
cfg := cfgManager.Get()
cfg.MessageAudit.AcknowledgedMessages.Enabled = true
cfg.MessageAudit.DeadLetteredMessages.Enabled = true
if _, err := cfgManager.Save(ctx, cfg); err != nil {
    log.Fatal(err)
}
```

## Pagination

```go
offset := int64(0)
for {
    result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
        Filter: queue.BrowsePublished,
        Offset: offset,
        Count:  50,
    })
    if err != nil {
        log.Fatal(err)
    }

    for _, id := range result.IDs {
        fmt.Println(id)
    }

    if !result.HasMore {
        break
    }
    offset += result.Count
}
```

## Result

```go
type BrowseResult struct {
    IDs     []string // Message IDs in this page
    Total   int64    // Total messages in this category
    Offset  int64    // Current offset
    Count   int64    // Number of IDs in this page
    HasMore bool     // Whether more pages exist
}
```

## Available Filters

| Filter                      | Redis Structure | Audit Required |
|-----------------------------|-----------------|----------------|
| `BrowsePublished`           | List            | No             |
| `BrowsePending` (FIFO/LIFO) | List            | No             |
| `BrowsePending` (Priority)  | Sorted Set      | No             |
| `BrowseScheduled`           | Sorted Set      | No             |
| `BrowseAcknowledged`        | List            | Yes            |
| `BrowseDeadLettered`        | List            | Yes            |

## Related

- [Message Management](message-management.md) — Get, delete, requeue by ID
- [Configuration](configuration.md) — Audit settings
