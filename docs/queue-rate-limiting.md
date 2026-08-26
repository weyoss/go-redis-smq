# Queue Rate Limiting

Control how fast messages are consumed from a queue. Protect downstream services, stay within API limits, or manage resource usage.

## Obtain Queue Manager

```go
import (
    "context"
    "log"
    "time"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

qm := redissmq.NewQueueManager()
```

The queue manager implements the public `queue.QueueManager` interface and is used for all rate limiting operations.

## Quick Start

```go
// Set a limit: 100 messages per minute
rl := queue.MustRateLimitParams(100, time.Minute)
err := qm.SetRateLimit(ctx, params, rl)
if err != nil {
    log.Fatal(err)
}
```

## Managing Rate Limits

### Set a Limit

```go
// 50 messages per 30 seconds
rl := queue.MustRateLimitParams(50, 30*time.Second)
err := qm.SetRateLimit(ctx, params, rl)

// Validate before creating
rl, err := queue.NewRateLimitParams(100, time.Minute)
if err != nil {
    log.Fatal(err)
}
```

### Get Current Limit

```go
rl, err := qm.RateLimit(ctx, params)
if err != nil {
    log.Fatal(err)
}
if rl != nil {
    fmt.Printf("Limit: %d per %s\n", rl.Limit(), rl.Interval())
} else {
    fmt.Println("No rate limit set")
}
```

### Clear a Limit

```go
err := qm.ClearRateLimit(ctx, params)
if err != nil {
    log.Fatal(err)
}
```

## Rate Limit Parameters

```go
type RateLimitParams struct {
    // limit: max messages (> 0)
    // interval: time window (>= 1 second)
}

// Create with validation
rl, err := queue.NewRateLimitParams(100, time.Minute)

// Create and panic on error (for known-valid values)
rl := queue.MustRateLimitParams(100, time.Minute)
```

## Common Patterns

### Protect External APIs

```go
// Don't exceed 10 requests per second
rl := queue.MustRateLimitParams(10, time.Second)
if err := qm.SetRateLimit(ctx, params, rl); err != nil {
    log.Fatal(err)
}
```

### Control Resource Usage

```go
// Limit to 5 messages per minute for CPU-heavy processing
rl := queue.MustRateLimitParams(5, time.Minute)
if err := qm.SetRateLimit(ctx, params, rl); err != nil {
    log.Fatal(err)
}
```

### Dynamic Adjustment

```go
// Read current limit
rl, err := qm.RateLimit(ctx, params)
if err != nil {
    log.Fatal(err)
}

// Adjust based on conditions
if isOffPeakHours() {
    rl = queue.MustRateLimitParams(1000, time.Minute)
} else {
    rl = queue.MustRateLimitParams(100, time.Minute)
}
if err := qm.SetRateLimit(ctx, params, rl); err != nil {
    log.Fatal(err)
}
```

## Error Handling

Sentinel errors are exported directly from the `queue` package.

```go
err := qm.SetRateLimit(ctx, params, rl)
if err != nil {
    switch {
    case errors.Is(err, queue.ErrNotFound):
        log.Println("Queue not found")
    case errors.Is(err, queue.ErrLocked):
        log.Println("Queue is locked")
    default:
        log.Printf("Unexpected error: %v", err)
    }
}
```

See [Error Handling](error-handling.md) for all error types.

## Validation

Rate limit parameters are validated on creation:

```go
rl, err := queue.NewRateLimitParams(0, time.Minute)           // error: limit must be > 0
rl, err = queue.NewRateLimitParams(100, 500*time.Millisecond) // error: interval >= 1 second
```

## Related

- [Queue Rate Limiting Concepts](https://github.com/weyoss/redis-smq-docs) — How rate limiting works
- [Queue Management](queue-management.md) — Queue CRUD operations
- [Error Handling](error-handling.md) — Error types
