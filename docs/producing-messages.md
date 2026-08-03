# Producing Messages

A Producer sends messages to a queue or through an exchange.

## Create and Start

```go
producer := redissmq.NewProducer()
if err := producer.Run(ctx); err != nil {
log.Fatal(err)
}
defer producer.Shutdown(ctx)
```

## Send a Message

### Direct to Queue

```go
m := msg.New().
SetBody(map[string]interface{}{"orderId": 123}).
SetQueue(ordersQueue)

ids, err := producer.Produce(ctx, m)
```

### Via Direct Exchange

```go
m := msg.New().
SetBody(data).
SetDirectExchange(exchangeParams).
SetExchangeRoutingKey("order.created")

ids, err := producer.Produce(ctx, m)
```

### Via Topic Exchange

```go
m := msg.New().
SetBody(data).
SetTopicExchange(exchangeParams).
SetExchangeRoutingKey("user.login.success")

ids, err := producer.Produce(ctx, m)
```

### Via Fanout Exchange

```go
m := msg.New().
SetBody(data).
SetFanoutExchange(exchangeParams)

ids, err := producer.Produce(ctx, m)
```

## Message Configuration

```go
m := msg.New().
SetBody(data).         // Any JSON-serializable value
SetQueue(queueParams). // Direct delivery
SetTTL(5 * time.Minute). // Expire after 5 minutes
SetPriority(msg.PriorityHigh). // For priority queues
SetRetryThreshold(3).          // Max 3 retries
SetRetryDelay(60 * time.Second). // Wait between retries
SetConsumeTimeout(30 * time.Second) // Max processing time
```

## Scheduling

```go
// Delay
m.SetScheduledDelay(10 * time.Second)

// CRON
m.SetScheduledCron("0 0 10 * * *")

// Repeat
m.SetScheduledRepeat(5)
m.SetScheduledRepeatPeriod(60 * time.Second)
```

See [Scheduling Messages](scheduling-messages.md) for details.

## Error Handling

```go
ids, err := producer.Produce(ctx, m)
if err != nil {
switch {
case errors.Is(err, q.ErrNotFound):
log.Println("Queue not found")
case errors.Is(err, q.ErrNotOperational):
log.Println("Queue is stopped")
case errors.Is(err, q.ErrLocked):
log.Println("Queue is locked")
case errors.Is(err, p.ErrNoMatchingQueues):
log.Println("No queues bound to routing key")
default:
log.Printf("Unexpected error: %v", err)
}
}
```

See [Error Handling](error-handling.md) for details.

## Related

- [Message Exchanges](../../../docs/message-exchanges.md) — Exchange concepts
- [Scheduling Messages](scheduling-messages.md) — Delays, CRON, repeating
- [Error Handling](error-handling.md) — Error types