# Queue Management

Create, inspect, and manage queues. Control state and rate limiting.

## Obtain Queue Managers

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

qm := redissmq.NewQueueManager()          // returns queue.QueueManager
sm := redissmq.NewStateManager()          // returns queue.StateManager
cgm := redissmq.NewConsumerGroupManager() // returns queue.ConsumerGroupManager
```

All managers are concrete implementations behind public interfaces. They are created once and can be used across your application.

## Create a Queue

```go
params := queue.MustQueueParams("orders")

// Standard queue
err := qm.Create(ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint)

// With rate limit
rl := queue.MustRateLimitParams(100, time.Minute)
err = qm.CreateWithRateLimit(ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint, rl)

// With namespace
params = queue.MustQueueParamsWithNS("orders", "production")
err = qm.Create(ctx, params, queue.TypeLIFO, queue.DeliveryPubSub)
```

## Queue Types

| Constant             | Description               |
|----------------------|---------------------------|
| `queue.TypeFIFO`     | First in, first out       |
| `queue.TypeLIFO`     | Last in, first out        |
| `queue.TypePriority` | Ordered by priority level |

## Delivery Models

| Constant                     | Description                  |
|------------------------------|------------------------------|
| `queue.DeliveryPointToPoint` | One consumer per message     |
| `queue.DeliveryPubSub`       | Broadcast to consumer groups |

## Inspect a Queue

```go
props, err := qm.Properties(ctx, params)
if err != nil {
    log.Fatal(err)
}
fmt.Println("Type:", props.Type)
fmt.Println("State:", props.OperationalState)
fmt.Println("Messages:", props.MessagesCount)
fmt.Println("Pending:", props.PendingMessagesCount)

exists, err := qm.Exists(ctx, params)
if err != nil {
    log.Fatal(err)
}
```

## Delete a Queue

```go
err := qm.Delete(ctx, params)
```

## Discovery

```go
all, err := qm.ListAll(ctx)
if err != nil {
    log.Fatal(err)
}

byNS, err := qm.ListByNamespace(ctx, "production")
if err != nil {
    log.Fatal(err)
}
```

## State Management

Use the state manager obtained from `redissmq.NewStateManager()`.

```go
// Pause
transition, err := sm.Pause(ctx, params, nil)

// Resume
transition, err = sm.Resume(ctx, params, nil)

// Stop
transition, err = sm.Stop(ctx, params, nil)

// Get current state
transition, err = sm.Current(ctx, params)

// Get state history
history, err := sm.History(ctx, params)
```

See [Queue State Management](queue-state-management.md) for details.

## Rate Limiting

```go
// Set
rl := queue.MustRateLimitParams(100, time.Minute)
err := qm.SetRateLimit(ctx, params, rl)

// Get
rl, err = qm.RateLimit(ctx, params)

// Clear
err = qm.ClearRateLimit(ctx, params)
```

See [Queue Rate Limiting](queue-rate-limiting.md) for details.

## Validation

The queue manager provides validation methods:

```go
err := qm.MustExist(ctx, params)           // Queue must exist
err = qm.MustBeOperational(ctx, params)    // Active or Paused
err = qm.CanEnqueue(ctx, params)           // Can accept messages
err = qm.CanDequeue(ctx, params)           // Can deliver messages
```

## Consumer Groups

For Pub/Sub queues, use the consumer group manager:

```go
// Create a group
result, err := cgm.Save(ctx, params, "email-service")

// Delete a group
err = cgm.Delete(ctx, params, "email-service")

// List groups
groups, err := cgm.List(ctx, params)
```

## Related

- [Queues](https://github.com/weyoss/redis-smq-docs) — Queue types and behavior
- [Queue Delivery Models](https://github.com/weyoss/redis-smq-docs) — Point-to-Point vs Pub/Sub
- [Message Browsing](message-browsing.md) — Browse queue messages
- [Queue State Management](queue-state-management.md) — Pause, resume, stop
- [Queue Rate Limiting](queue-rate-limiting.md) — Rate limits
