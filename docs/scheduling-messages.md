# Scheduling Messages

Schedule messages for future delivery using delays, CRON expressions, or repeating patterns.

## One-Time Delay

```go
m := msg.New().
SetQueue(queueParams).
SetBody(data).
SetScheduledDelay(30 * time.Second)
```

## CRON Schedule

```go
m := msg.New().
SetQueue(queueParams).
SetBody(data).
SetScheduledCron("0 30 9 * * 1-5") // Weekdays at 9:30 AM
```

Invalid CRON expressions are silently ignored.

## Repeating Delivery

```go
m := msg.New().
SetQueue(queueParams).
SetBody(data).
SetScheduledDelay(10 * time.Second). // First after 10s
SetScheduledRepeat(5).                     // Repeat 5 times
SetScheduledRepeatPeriod(60 * time.Second) // Every 60s
```

A repeat count of `0` means repeat indefinitely.

## Clear Scheduling

```go
m.ResetScheduledParams()
```

## Browse Scheduled Messages

```go
result, err := queue.BrowseMessages(ctx, params, &q.BrowseParams{
Filter: q.BrowseScheduled,
Offset: 0,
Count:  100,
})
```

See [Message Browsing](message-browsing.md) for details.

## Related

- [Scheduling Messages Concepts](../../../docs/scheduling-messages.md) — How scheduling works
- [Producing Messages](producing-messages.md) — Publishing messages
- [Message Browsing](message-browsing.md) — Viewing scheduled messages