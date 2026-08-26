# Error Handling

RedisSMQ uses sentinel errors for known conditions. Use `errors.Is` to check error types. All errors are exported directly from their domain package; there are no separate `x` or `cfg` subpackages.

## Queue Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

errors.Is(err, queue.ErrNotFound)                // Queue doesn't exist
errors.Is(err, queue.ErrAlreadyExists)           // Queue name taken
errors.Is(err, queue.ErrNotOperational)          // Queue is stopped or paused
errors.Is(err, queue.ErrLocked)                  // Queue is locked
errors.Is(err, queue.ErrRateLimitExceeded)       // Rate limit reached
errors.Is(err, queue.ErrQueueNotEmpty)           // Can't delete non-empty queue
errors.Is(err, queue.ErrQueueHasActiveConsumers) // Active consumers on queue
errors.Is(err, queue.ErrQueueHasBoundExchanges)  // Exchange bindings exist
errors.Is(err, queue.ErrAuditDisabled)           // Audit not enabled for this operation
```

## State Transition Errors

```go
errors.Is(err, queue.ErrInvalidTransition)   // Invalid state change
errors.Is(err, queue.ErrInvalidLock)         // Bad lock parameters
errors.Is(err, queue.ErrLockOwnerMismatch)   // Wrong lock owner
errors.Is(err, queue.ErrLockIDMismatch)      // Wrong lock ID
errors.Is(err, queue.ErrNotLocked)           // Queue isn't locked
```

## Producer Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/producer"

errors.Is(err, producer.ErrNotRunning)            // Producer not started
errors.Is(err, producer.ErrExchangeRequired)      // Missing queue or exchange
errors.Is(err, producer.ErrRoutingKeyRequired)    // Missing routing key
errors.Is(err, producer.ErrNoMatchingQueues)      // No queues for routing key
errors.Is(err, producer.ErrMessageAlreadyExists)  // Duplicate message ID
errors.Is(err, producer.ErrPriorityRequired)      // Missing priority on message
errors.Is(err, producer.ErrPriorityNotEnabled)    // Queue doesn't support priority
errors.Is(err, producer.ErrUnknownQueueType)      // Unrecognized queue type
errors.Is(err, producer.ErrInvalidQueueState)     // Queue in invalid state
```

## Consumer Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/consumer"

errors.Is(err, consumer.ErrNoQueues)           // No queues registered
errors.Is(err, consumer.ErrQueueStopped)       // Queue is stopped
errors.Is(err, consumer.ErrQueueLocked)        // Queue is locked
errors.Is(err, consumer.ErrQueueInvalidState)  // Invalid queue state
```

## Message Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/message"

errors.Is(err, message.ErrNotFound)               // Message not found
errors.Is(err, message.ErrNotRequeuable)          // Cannot requeue this message
```

## Exchange Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/exchange"

errors.Is(err, exchange.ErrNotFound)            // Exchange not found
errors.Is(err, exchange.ErrAlreadyExists)       // Exchange already exists
errors.Is(err, exchange.ErrQueueAlreadyBound)   // Queue already bound
errors.Is(err, exchange.ErrQueueNotBound)       // Queue not bound
errors.Is(err, exchange.ErrHasBoundQueues)      // Cannot delete — queues bound
errors.Is(err, exchange.ErrInvalidPattern)      // Invalid topic pattern
errors.Is(err, exchange.ErrInvalidRoutingKey)   // Invalid routing key
errors.Is(err, exchange.ErrNamespaceMismatch)   // Queue and exchange in different namespaces
errors.Is(err, exchange.ErrTypeMismatch)        // Exchange type mismatch
errors.Is(err, exchange.ErrPolicyViolation)     // Queue policy violation
```

## Namespace Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/namespace"

errors.Is(err, namespace.ErrNotFound)     // Namespace not found
errors.Is(err, namespace.ErrInvalidName)  // Invalid namespace name
errors.Is(err, namespace.ErrNameRequired) // Namespace name is empty
```

## Config Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/config"

errors.Is(err, config.ErrNotInitialized)   // Config not initialized
errors.Is(err, config.ErrVersionMismatch)  // Modified by another instance
errors.Is(err, config.ErrInvalidConfig)    // Invalid configuration
```

## Common Patterns

### Checking Specific Errors

```go
ids, err := producer.Produce(ctx, m)
if err != nil {
    switch {
    case errors.Is(err, queue.ErrNotFound):
        log.Println("Queue not found — create it first")
    case errors.Is(err, queue.ErrNotOperational):
        log.Println("Queue is stopped — resume it first")
    case errors.Is(err, producer.ErrNoMatchingQueues):
        log.Println("No queues bound to this routing key")
    default:
        log.Printf("Unexpected error: %v", err)
    }
}
```

### Handling Version Mismatch

```go
version, err := cfgManager.Save(ctx, cfg)
if errors.Is(err, config.ErrVersionMismatch) {
    // Re-read and retry
    cfg = cfgManager.Get()
    cfg.Logger.Enabled = true
    version, err = cfgManager.Save(ctx, cfg)
}
```

## Related

- [Configuration](configuration.md) — Configuration manager
- [Queue Management](queue-management.md) — Queue errors
- [Producing Messages](producing-messages.md) — Producer errors
- [Consuming Messages](consuming-messages.md) — Consumer errors
