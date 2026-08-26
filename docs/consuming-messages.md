# Consuming Messages

A Consumer processes messages from queues. Provide a handler function that receives each message and returns an error to trigger retry.

## Create and Start

```go
import (
    "context"
    "log"
    "time"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/consumer"
    "github.com/weyoss/go-redis-smq/pkg/message"
)

consumer := redissmq.NewConsumer(
    consumer.WithHeartbeatTTL(30 * time.Second),
)

consumer.Consume(ordersQueue, func(ctx context.Context, m *message.Transferable) error {
    log.Printf("Received: %v", m.Body)
    return nil // success
})

if err := consumer.Run(ctx); err != nil {
    log.Fatal(err)
}
defer consumer.Shutdown()
```

## Message Handler

The handler receives a `*message.Transferable` and returns an error:

```go
consumer.Consume(queueParams, func(ctx context.Context, m *message.Transferable) error {
    if err := processOrder(m.Body); err != nil {
        return err // triggers retry
    }
    return nil // acknowledge
})
```

## Pub/Sub with Consumer Groups

```go
consumer.ConsumeWithGroup(queueParams, "email-service", func(ctx context.Context, m *message.Transferable) error {
    // Only one consumer in "email-service" gets this message
    return nil
})
```

See [Consumer Groups](consumer-groups.md) for details.

## Configuration

### Heartbeat

```go
consumer := redissmq.NewConsumer(
    consumer.WithHeartbeatTTL(60 * time.Second),
)
```

If heartbeats stop, the consumer is considered dead and its in‑flight messages are recovered.

### Batch Acknowledgments

Group acknowledgments into a single Redis operation for higher throughput:

```go
import "github.com/weyoss/go-redis-smq/pkg/consumer"

// Enable with defaults (100 messages or 10 seconds)
consumer := redissmq.NewConsumer(
    consumer.WithBatchAcks(consumer.BatchConfig{Enabled: true}),
)

// Custom settings
consumer := redissmq.NewConsumer(
    consumer.WithBatchAcks(consumer.BatchConfig{
        Enabled:      true,
        BatchSize:    500,
        BatchTimeout: 5 * time.Second,
    }),
)
```

A batch is flushed when:

- The batch is full (reaches `BatchSize`)
- The timeout expires (`BatchTimeout` since first message)
- The consumer shuts down (all pending acknowledgments are flushed)

Disabled by default — each message is acknowledged immediately.

### Batch Unacknowledgments

Same pattern for failed messages:

```go
consumer := redissmq.NewConsumer(
    consumer.WithBatchUnacks(consumer.BatchConfig{
        Enabled:      true,
        BatchSize:    50,
        BatchTimeout: 5 * time.Second,
    }),
)
```

## Message Object

The `*message.Transferable` provides:

```go
m.ID   // Unique message identifier
m.Body // The payload
m.TTL              // Time-to-live in milliseconds
m.RetryThreshold   // Max retry attempts
m.RetryDelay       // Delay between retries in ms
m.ConsumeTimeout  // Max processing time in ms
m.Priority        // Priority level (if set)
m.Status          // Current message status
m.CreatedAt       // Creation timestamp (Unix ms)
m.DestinationQueue // The target queue
m.MessageState    // Lifecycle state (attempts, timestamps)
```

## Shutdown

```go
// Graceful shutdown — pending acknowledgments are flushed
consumer.Shutdown()

// Or use system shutdown
redissmq.Shutdown()
```

## Related

- [Consumer Groups](consumer-groups.md) — Pub/Sub with groups
- [Graceful Shutdown](graceful-shutdown.md) — Clean shutdown
- [Error Handling](error-handling.md) — Consumer error types
- [Message Reliability](https://github.com/weyoss/redis-smq-docs) — Delivery guarantees
