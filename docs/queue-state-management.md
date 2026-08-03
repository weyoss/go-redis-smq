# Queue State Management

Control and track the operational state of queues. Pause processing, stop queues entirely, resume normal operation, and view state history.

## Quick Start

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

// Pause a queue (uses default reason MANUAL)
transition, err := queue.Pause(ctx, params, nil)

// Resume a queue
transition, err := queue.Resume(ctx, params, nil)
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
transition, err := queue.Pause(ctx, params, &q.StateTransitionOptions{
    Reason:      ptr(q.ReasonManual),       // user‑facing constant
    Description: ptr("Scheduled database maintenance"),
})
```

### Resume a Queue

Resumes processing from Paused or Stopped state:

```go
transition, err := queue.Resume(ctx, params, &q.StateTransitionOptions{
    Reason: ptr(q.ReasonManual),
})
```

### Stop a Queue

Completely halts the queue:

```go
transition, err := queue.Stop(ctx, params, &q.StateTransitionOptions{
    Reason:      ptr(q.ReasonEmergency),
    Description: ptr("Critical system error"),
})
```

### Get Current State

```go
transition, err := queue.Current(ctx, params)
if err != nil {
    log.Fatal(err)
}
fmt.Println("State:", transition.To)
fmt.Println("Since:", time.UnixMilli(transition.Timestamp))
fmt.Println("Reason:", transition.Reason)  // QueueStateTransitionReason (string)
```

### Get State History

```go
history, err := queue.History(ctx, params)
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

| Option        | Type                           | Description                         |
|---------------|--------------------------------|-------------------------------------|
| `Reason`      | `*StateTransitionReason`       | Why the state changed (user‑facing constant) |
| `Description` | `*string`                      | Human-readable explanation          |
| `Metadata`    | `map[string]interface{}`       | Arbitrary key-value data            |

**Available user‑facing reasons** (constants from package `q`):  
`q.ReasonManual`, `q.ReasonScheduled`, `q.ReasonEmergency`, `q.ReasonPerformance`, `q.ReasonError`, `q.ReasonConfigChange`, `q.ReasonTesting`, `q.ReasonOther`.

System‑internal reasons (`ReasonSystemInit`, `ReasonPurgeStart`, etc.) are not accessible through the public API.

## State Transition Rules

```
Active  → Paused, Stopped, Locked
Paused  → Active, Stopped, Locked
Stopped → Active
Locked  → Active, Stopped
```

Invalid transitions return `ErrInvalidTransition`.

## Lock and Unlock

Locks are used internally for maintenance operations. Users typically interact via Pause/Resume/Stop:

```go
// Lock a queue (requires owner and lock ID)
transition, err := queue.Lock(ctx, params, q.LockOwnerPurgeJob, "purge-123", nil)

// Unlock a queue (must match owner and lock ID)
transition, err := queue.Unlock(ctx, params, q.LockOwnerPurgeJob, "purge-123", nil)
```

## Using with StateManager

The `StateManager` provides the same methods with an explicit instance:

```go
sm := queue.NewStateManager()

transition, err := sm.Pause(ctx, params, nil)
transition, err := sm.Current(ctx, params)
history, err := sm.History(ctx, params)
```

Package-level convenience functions use a default `StateManager` internally.

## Listening to State Changes

State changes are published as events:

```go
import queueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"

sub, _ := queueEvents.SubscribeStateChanged(func(p queueEvents.StateChangedPayload) {
    fmt.Printf("Queue %s → %s\n", p.Queue.String(), p.Transition.To)
})
defer sub.Unsubscribe()
```

## Error Handling

```go
transition, err := queue.Pause(ctx, params, nil)
if err != nil {
    switch {
    case errors.Is(err, q.ErrNotFound):
        log.Println("Queue not found")
    case errors.Is(err, q.ErrInvalidTransition):
        log.Println("Invalid state transition")
    default:
        log.Printf("Unexpected error: %v", err)
    }
}
```

See [Error Handling](error-handling.md) for all state-related errors.

## Related

- [Queue State Management Concepts](../../../docs/queue-state-management.md) — How state management works
- [Queue Management](queue-management.md) — Queue CRUD operations
- [Error Handling](error-handling.md) — Error types

