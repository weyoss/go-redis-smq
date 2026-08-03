# Error Handling

RedisSMQ uses sentinel errors for known conditions. Use `errors.Is` to check error types. State transition reasons are now strongly typed; passing a raw string to `StateTransitionOptions.Reason` will cause a compilation error. Use the predefined constants from package `q`.

## Queue Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/queue/q"

errors.Is(err, q.ErrNotFound)                // Queue doesn't exist
errors.Is(err, q.ErrAlreadyExists)           // Queue name taken
errors.Is(err, q.ErrNotOperational)          // Queue is stopped or paused
errors.Is(err, q.ErrLocked)                  // Queue is locked
errors.Is(err, q.ErrRateLimitExceeded)       // Rate limit reached
errors.Is(err, q.ErrQueueNotEmpty)           // Can't delete non-empty queue
errors.Is(err, q.ErrQueueHasActiveConsumers) // Active consumers on queue
errors.Is(err, q.ErrQueueHasBoundExchanges)  // Exchange bindings exist
errors.Is(err, q.ErrAuditDisabled)           // Audit not enabled for this operation
```

## State Transition Errors

```go
errors.Is(err, q.ErrInvalidTransition)   // Invalid state change
errors.Is(err, q.ErrInvalidLock)         // Bad lock parameters
errors.Is(err, q.ErrLockOwnerMismatch)   // Wrong lock owner
errors.Is(err, q.ErrLockIDMismatch)      // Wrong lock ID
errors.Is(err, q.ErrNotLocked)           // Queue isn't locked
```

> **Note:** The `Reason` field in `StateTransitionOptions` now expects a `*StateTransitionReason` (one of the user‑facing constants). Using a system reason or a plain string is a compile‑time error, so no runtime error is defined for it.

## Producer Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/producer/p"

errors.Is(err, p.ErrNotRunning)            // Producer not started
errors.Is(err, p.ErrExchangeRequired)      // Missing queue or exchange
errors.Is(err, p.ErrRoutingKeyRequired)    // Missing routing key
errors.Is(err, p.ErrNoMatchingQueues)      // No queues for routing key
errors.Is(err, p.ErrMessageAlreadyExists)  // Duplicate message ID
errors.Is(err, p.ErrPriorityRequired)      // Missing priority on message
errors.Is(err, p.ErrPriorityNotEnabled)    // Queue doesn't support priority
errors.Is(err, p.ErrUnknownQueueType)      // Unrecognized queue type
errors.Is(err, p.ErrInvalidQueueState)     // Queue in invalid state
```

## Consumer Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/consumer/c"

errors.Is(err, c.ErrNoQueues)           // No queues registered
errors.Is(err, c.ErrQueueStopped)       // Queue is stopped
errors.Is(err, c.ErrQueueLocked)        // Queue is locked
errors.Is(err, c.ErrQueueInvalidState)  // Invalid queue state
```

## Message Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/message/msg"

errors.Is(err, msg.ErrNotFound)               // Message not found
errors.Is(err, msg.ErrNotRequeuable)          // Cannot requeue this message
errors.Is(err, msg.ErrMessageExpired)         // TTL elapsed
errors.Is(err, msg.ErrRetryThresholdExceeded) // Max retries reached
```

## Exchange Errors

```go
import "github.com/weyoss/go-redis-smq/pkg/exchange/x"

errors.Is(err, x.ErrNotFound)            // Exchange not found
errors.Is(err, x.ErrAlreadyExists)       // Exchange already exists
errors.Is(err, x.ErrQueueAlreadyBound)   // Queue already bound
errors.Is(err, x.ErrQueueNotBound)       // Queue not bound
errors.Is(err, x.ErrHasBoundQueues)      // Cannot delete — queues bound
errors.Is(err, x.ErrInvalidPattern)      // Invalid topic pattern
errors.Is(err, x.ErrInvalidRoutingKey)   // Invalid routing key
errors.Is(err, x.ErrNamespaceMismatch)   // Queue and exchange in different namespaces
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
import "github.com/weyoss/go-redis-smq/pkg/config/cfg"

errors.Is(err, cfg.ErrNotInitialized)   // Config not initialized
errors.Is(err, cfg.ErrVersionMismatch)  // Modified by another instance
errors.Is(err, cfg.ErrInvalidConfig)    // Invalid configuration
```

## Custom Error Types

Exchange type mismatches and policy violations use custom error types:

```go
import "github.com/weyoss/go-redis-smq/pkg/exchange/x"

var typeErr *x.TypeMismatchError
if errors.As(err, &typeErr) {
    fmt.Println("Expected:", typeErr.Expected)
    fmt.Println("Actual:", typeErr.Actual)
}

var policyErr *x.PolicyViolationError
if errors.As(err, &policyErr) {
    fmt.Println("Policy:", policyErr.Policy)
    fmt.Println("Allowed:", policyErr.AllowedKinds)
    fmt.Println("Actual:", policyErr.ActualKind)
}
```

## Common Patterns

### Checking Specific Errors

```go
ids, err := producer.Produce(ctx, m)
if err != nil {
    switch {
    case errors.Is(err, q.ErrNotFound):
        log.Println("Queue not found — create it first")
    case errors.Is(err, q.ErrNotOperational):
        log.Println("Queue is stopped — resume it first")
    case errors.Is(err, p.ErrNoMatchingQueues):
        log.Println("No queues bound to this routing key")
    default:
        log.Printf("Unexpected error: %v", err)
    }
}
```

### Handling Version Mismatch

```go
version, err := config.Save(ctx, cfg)
if errors.Is(err, cfg.ErrVersionMismatch) {
// Re-read and retry
cfg = config.Get()
cfg.Logger.Enabled = true
version, err = config.Save(ctx, cfg)
}
```