# Queue State Management

Control and track the operational state of queues. Pause processing, stop queues entirely, resume normal operation, and view state history.

## Obtain State Manager

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

sm := redissmq.NewStateManager()
```

The state manager implements the public `queue.StateManager` interface and is used for all state transition operations.

## Quick Start

```go
// Pause a queue (uses default reason MANUAL)
transition, err := sm.Pause(ctx, params, nil)

// Resume a queue
transition, err = sm.Resume(ctx, params, nil)
```

## States

| State       | Accepts Messages | Delivers Messages | Description                        |
|-------------|------------------|-------------------|------------------------------------|
| **Active**  | Yes              | Yes               | Normal operation                   |
| **Paused**  | Yes              | No                | Buffers messages, stops processing |
| **Stopped** | No               | No                | Fully halted                       |
| **Locked**  | No               | No                | Exclusive maintenance (internal)   |

## Managing State

### Pause a Queue

Temporarily stops processing while accepting new messages:

```go
transition, err := sm.Pause(ctx, params, &queue.StateTransitionOptions{
    Reason:      ptr(queue.ReasonManual),
    Description: ptr("Scheduled database maintenance"),
})
```

### Resume a Queue

Resumes processing from Paused or Stopped state:

```go
transition, err := sm.Resume(ctx, params, &queue.StateTransitionOptions{
    Reason: ptr(queue.ReasonManual),
})
```

### Stop a Queue

Completely halts the queue:

```go
transition, err := sm.Stop(ctx, params, &queue.StateTransitionOptions{
    Reason:      ptr(queue.ReasonEmergency),
    Description: ptr("Critical system error"),
})
```

### Get Current State

```go
transition, err := sm.Current(ctx, params)
if err != nil {
    log.Fatal(err)
}
fmt.Println("State:", transition.To)
fmt.Println("Since:", time.UnixMilli(transition.Timestamp))
fmt.Println("Reason:", transition.Reason)
```

### Get State History

```go
history, err := sm.History(ctx, params)
if err != nil {
    log.Fatal(err)
}
for _, t := range history {
    from := "INITIAL"
    if t.From != nil {
        from = t.From.String()
    }
    fmt.Printf("%s → %s (%s)\n", from, t.To, t.Reason)
}
```

## Transition Options

Each state change can include:

| Option        | Type                           | Description                                  |
|---------------|--------------------------------|----------------------------------------------|
| `Reason`      | `*queue.StateTransitionReason` | Why the state changed (user‑facing constant) |
| `Description` | `*string`                      | Human-readable explanation                   |
| `Metadata`    | `map[string]interface{}`       | Arbitrary key-value data                     |

**Available user‑facing reasons** (constants from package `queue`):  
`queue.ReasonManual`, `queue.ReasonScheduled`, `queue.ReasonEmergency`, `queue.ReasonPerformance`, `queue.ReasonError`, `queue.ReasonConfigChange`, `queue.ReasonTesting`, `queue.ReasonOther`.

System‑internal reasons (`ReasonSystemInit`, `ReasonPurgeStart`, etc.) are not accessible through the public API.

## State Transition Rules

```
Active  → Paused, Stopped, Locked
Paused  → Active, Stopped, Locked
Stopped → Active
Locked  → Active, Stopped
```

Invalid transitions return `queue.ErrInvalidTransition`.

## Lock and Unlock

Locks are used internally for maintenance operations. Users typically interact via Pause/Resume/Stop:

```go
// Lock a queue (requires owner and lock ID)
transition, err := sm.Lock(ctx, params, queue.LockOwnerPurgeJob, "purge-123", nil)

// Unlock a queue (must match owner and lock ID)
transition, err = sm.Unlock(ctx, params, queue.LockOwnerPurgeJob, "purge-123", nil)
```

## Error Handling

Sentinel errors are exported directly from the `queue` package.

```go
transition, err := sm.Pause(ctx, params, nil)
if err != nil {
    switch {
    case errors.Is(err, queue.ErrNotFound):
        log.Println("Queue not found")
    case errors.Is(err, queue.ErrInvalidTransition):
        log.Println("Invalid state transition")
    case errors.Is(err, queue.ErrNotLocked):
        log.Println("Queue is not locked")
    default:
        log.Printf("Unexpected error: %v", err)
    }
}
```

See [Error Handling](error-handling.md) for all state-related errors.

## Related

- [Queue State Management Concepts](https://github.com/weyoss/redis-smq-docs) — How state management works
- [Queue Management](queue-management.md) — Queue CRUD operations
- [Error Handling](error-handling.md) — Error types
