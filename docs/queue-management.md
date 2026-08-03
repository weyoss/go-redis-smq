# Queue Management

Create, inspect, and manage queues. Control state and rate limiting.

## Create a Queue

```go
import (
"github.com/weyoss/go-redis-smq/pkg/queue"
"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

params := q.MustQueueParams("orders")

// Standard queue
err := queue.Create(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint)

// With rate limit
rl := q.MustRateLimitParams(100, time.Minute)
err := queue.CreateWithRateLimit(ctx, params, q.TypeFIFO, q.DeliveryPointToPoint, rl)

// With namespace
params := q.MustQueueParamsWithNS("orders", "production")
err := queue.Create(ctx, params, q.TypeLIFO, q.DeliveryPubSub)
```

## Queue Types

| Constant         | Description               |
|------------------|---------------------------|
| `q.TypeFIFO`     | First in, first out       |
| `q.TypeLIFO`     | Last in, first out        |
| `q.TypePriority` | Ordered by priority level |

## Delivery Models

| Constant                 | Description                  |
|--------------------------|------------------------------|
| `q.DeliveryPointToPoint` | One consumer per message     |
| `q.DeliveryPubSub`       | Broadcast to consumer groups |

## Inspect a Queue

```go
props, err := queue.Properties(ctx, params)
fmt.Println("Type:", props.Type)
fmt.Println("State:", props.OperationalState)
fmt.Println("Messages:", props.MessagesCount)
fmt.Println("Pending:", props.PendingMessagesCount)

exists, err := queue.Exists(ctx, params)
```

## Delete a Queue

```go
err := queue.Delete(ctx, params)
```

## Discovery

```go
all, err := queue.ListAll(ctx)
byNS, err := queue.ListByNamespace(ctx, "production")
```

## State Management

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

// Pause
transition, err := queue.Pause(ctx, params, nil)

// Resume
transition, err := queue.Resume(ctx, params, nil)

// Stop
transition, err := queue.Stop(ctx, params, nil)

// Get current state
transition, err := queue.Current(ctx, params)

// Get state history
history, err := queue.History(ctx, params)
```

See [Queue State Management](../../../docs/queue-state-management.md) for concepts.

## Rate Limiting

```go
// Set
rl := q.MustRateLimitParams(100, time.Minute)
err := queue.SetRateLimit(ctx, params, rl)

// Get
rl, err := queue.RateLimit(ctx, params)

// Clear
err := queue.ClearRateLimit(ctx, params)
```

See [Queue Rate Limiting](../../../docs/queue-rate-limiting.md) for concepts.

## Validation

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

err := queue.MustExist(ctx, params) // Queue must exist
err := queue.MustBeOperational(ctx, params) // Active or Paused
err := queue.CanEnqueue(ctx, params) // Can accept messages
err := queue.CanDequeue(ctx, params) // Can deliver messages
```

## Related

- [Queues](../../../docs/queues.md) — Queue types and behavior
- [Queue Delivery Models](../../../docs/queue-delivery-models.md) — Point-to-Point vs Pub/Sub
- [Message Browsing](message-browsing.md) — Browse queue messages