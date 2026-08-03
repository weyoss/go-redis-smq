# Queue Rate Limiting

Control how fast messages are consumed from a queue. Protect downstream services, stay within API limits, or manage
resource usage.

## Quick Start

```go
import (
"time"
"github.com/weyoss/go-redis-smq/pkg/queue"
"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// Set a limit: 100 messages per minute
rl := q.MustRateLimitParams(100, time.Minute)
err := queue.SetRateLimit(ctx, params, rl)
```

## Managing Rate Limits

### Set a Limit

```go
// 50 messages per 30 seconds
rl := q.MustRateLimitParams(50, 30*time.Second)
err := queue.SetRateLimit(ctx, params, rl)

// Validate before creating
rl, err := q.NewRateLimitParams(100, time.Minute)
if err != nil {
log.Fatal(err)
}
```

### Get Current Limit

```go
rl, err := queue.RateLimit(ctx, params)
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
err := queue.ClearRateLimit(ctx, params)
```

## Rate Limit Parameters

```go
type RateLimitParams struct {
// limit: max messages (> 0)
// interval: time window (>= 1 second)
}

// Create with validation
rl, err := q.NewRateLimitParams(100, time.Minute)

// Create and panic on error (for known-valid values)
rl := q.MustRateLimitParams(100, time.Minute)
```

## Common Patterns

### Protect External APIs

```go
// Don't exceed 10 requests per second
rl := q.MustRateLimitParams(10, time.Second)
queue.SetRateLimit(ctx, params, rl)
```

### Control Resource Usage

```go
// Limit to 5 messages per minute for CPU-heavy processing
rl := q.MustRateLimitParams(5, time.Minute)
queue.SetRateLimit(ctx, params, rl)
```

### Dynamic Adjustment

```go
// Read current limit
rl, _ := queue.RateLimit(ctx, params)

// Adjust based on conditions
if isOffPeakHours() {
rl = q.MustRateLimitParams(1000, time.Minute)
} else {
rl = q.MustRateLimitParams(100, time.Minute)
}
queue.SetRateLimit(ctx, params, rl)
```

## Error Handling

```go
err := queue.SetRateLimit(ctx, params, rl)
if err != nil {
switch {
case errors.Is(err, q.ErrNotFound):
log.Println("Queue not found")
case errors.Is(err, q.ErrLocked):
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
rl, err := q.NewRateLimitParams(0, time.Minute) // error: limit must be > 0
rl, err := q.NewRateLimitParams(100, 500*time.Millisecond) // error: interval >= 1 second
```

## Related

- [Queue Rate Limiting Concepts](../../../docs/queue-rate-limiting.md) — How rate limiting works
- [Queue Management](queue-management.md) — Queue CRUD operations
- [Error Handling](error-handling.md) — Error types
