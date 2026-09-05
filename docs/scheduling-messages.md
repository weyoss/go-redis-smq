# Scheduling Messages

Schedule messages for future delivery using delays, CRON expressions, or repeating patterns.

## Quick Start

```go
import (
    "time"

    "github.com/weyoss/go-redis-smq/pkg/message"
)

// One-time delay
m := message.New().
    SetQueue(queueParams).
    SetBody(data).
    SetScheduledDelay(30 * time.Second)
```

## One-Time Delay

Deliver a message after a fixed delay:

```go
m := message.New().
    SetQueue(queueParams).
    SetBody(data).
    SetScheduledDelay(10 * time.Second)
```

The message is stored in the scheduled queue and moved to pending when the delay elapses.

## CRON Schedule

Schedule using standard CRON expressions. Both 5-field and 6-field formats are supported:

- **5 fields:** minute hour day-of-month month day-of-week  
  Example: `"30 9 * * 1-5"` — Weekdays at 9:30 AM (seconds default to 0)
- **6 fields:** second minute hour day-of-month month day-of-week  
  Example: `"0 30 9 * * 1-5"` — Weekdays at 9:30:00 AM

```go
m := message.New().
    SetQueue(queueParams).
    SetBody(data).
    SetScheduledCron("30 9 * * 1-5") // Weekdays at 9:30 AM
```

Invalid CRON expressions (wrong field count or invalid syntax) are rejected and the message will not be scheduled; if no other scheduling is configured, the message is treated as immediate.

## Repeating Delivery

Repeat a message a fixed number of times (or indefinitely) after an initial delay.

```go
m := message.New().
    SetQueue(queueParams).
    SetBody(data).
    SetScheduledDelay(10 * time.Second).      // First delivery after 10s
    SetScheduledRepeat(5).                    // Repeat 5 times after the first delivery
    SetScheduledRepeatPeriod(60 * time.Second) // Every 60 seconds between repeats
```

- `SetScheduledRepeat(0)` means repeat indefinitely.
- If `SetScheduledRepeatPeriod` is not set, repeats occur immediately (or with the same delay as the first delivery) depending on implementation.

## Clearing Scheduling

Remove all scheduling parameters from a message:

```go
m.ResetScheduledParams()
```

After reset, the message is treated as immediate.

## Browse Scheduled Messages

Use the queue manager to browse scheduled messages:

```go
qm := redissmq.NewQueueManager()

result, err := qm.BrowseMessages(ctx, params, &queue.BrowseParams{
    Filter: queue.BrowseScheduled,
    Offset: 0,
    Count:  100,
})
if err != nil {
    log.Fatal(err)
}
```

See [Message Browsing](message-browsing.md) for details.

## Scheduling Rules

- **Delay** takes precedence over CRON and repeat for the *first* delivery.
- **CRON + Repeat** — the repeat period is used between CRON-triggered deliveries, but the first delivery occurs at the next CRON tick (unless a delay is set, which overrides the first tick).
- **Repeat only** — first delivery is immediate if no delay, subsequent deliveries follow the repeat period.

## Related

- [Scheduling Messages Concepts](https://github.com/weyoss/redis-smq-docs) — How scheduling works
- [Producing Messages](producing-messages.md) — Publishing messages
- [Message Browsing](message-browsing.md) — Viewing scheduled messages
