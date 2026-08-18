# Event Bus

RedisSMQ exposes a public event bus that lets you observe system activity in real time. Events are delivered over Redis Pub/Sub and are intended for monitoring, alerting, and integration.

The public event bus is separate from the internal system bus used for cross‑instance synchronisation. Public events are safe to use from outside the RedisSMQ package.

## Quick Start

No manual initialisation is required. The first call to any public subscription function automatically starts the public user event bus. `redissmq.Shutdown()` stops it automatically.

```go
import (
    "context"

    "github.com/weyoss/go-redis-smq"
    queueEvents "github.com/weyoss/go-redis-smq/pkg/queue/events"
)

func main() {
    ctx := context.Background()
    if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
        panic(err)
    }
    defer redissmq.Shutdown()

    sub, err := queueEvents.SubscribeCreated(func(p queueEvents.CreatedPayload) {
        log.Printf("Queue created: %s", p.Queue.Name())
    })
    if err != nil {
        panic(err)
    }
    defer sub.Unsubscribe()
}
```

## Subscribing to Events

Subscription functions live in domain‑specific packages:

| Package | Events |
|---------|--------|
| `pkg/queue/events` | Queue lifecycle and state changes |
| `pkg/consumer/events` | Consumer lifecycle and message processing |
| `pkg/producer/events` | Producer lifecycle and message publication |

All subscription functions return a `*eventbus.Subscription`. Call `Unsubscribe()` when you no longer need to receive events.

## Available Events

### Queue Events

#### `queue.queueCreated`

Fires when a new queue is created.

```go
sub, _ := queueEvents.SubscribeCreated(func(p queueEvents.CreatedPayload) {
    fmt.Println("Queue:", p.Queue.Name())
})
```

Payload:

| Field | Type | Description |
|-------|------|-------------|
| `p.Queue` | `q.QueueParams` | The queue that was created |
| `p.Properties` | `q.QueueProps` | Queue properties (type, delivery model, etc.) |

#### `queue.queueDeleted`

Fires when a queue is deleted.

```go
sub, _ := queueEvents.SubscribeDeleted(func(p queueEvents.DeletedPayload) {
    fmt.Println("Deleted queue:", p.Queue.Name())
})
```

Payload:

| Field | Type |
|-------|------|
| `p.Queue` | `q.QueueParams` |

#### `queue.stateChanged`

Fires when the operational state of a queue changes.

```go
sub, _ := queueEvents.SubscribeStateChanged(func(p queueEvents.StateChangedPayload) {
    fmt.Printf("Queue %s changed from %v to %v\n",
        p.Queue.Name(), p.Transition.From, p.Transition.To)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.Queue` | `q.QueueParams` |
| `p.Transition` | `q.StateTransition` |

#### `queue.consumerGroupCreated`

Fires when a consumer group is created for a Pub/Sub queue.

```go
sub, _ := queueEvents.SubscribeConsumerGroupCreated(func(p queueEvents.ConsumerGroupCreatedPayload) {
    fmt.Println("Group:", p.GroupID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.Queue` | `q.QueueParams` |
| `p.GroupID` | `string` |

#### `queue.consumerGroupDeleted`

Fires when a consumer group is deleted.

```go
sub, _ := queueEvents.SubscribeConsumerGroupDeleted(func(p queueEvents.ConsumerGroupDeletedPayload) {
    fmt.Println("Deleted group:", p.GroupID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.Queue` | `q.QueueParams` |
| `p.GroupID` | `string` |

### Producer Events

#### `producer.up`, `producer.down`, `producer.goingUp`, `producer.goingDown`

Fires when a producer starts, stops, or begins a lifecycle transition.

```go
sub, _ := producerEvents.SubscribeUp(func(p producerEvents.LifecyclePayload) {
    fmt.Println("Producer started:", p.ProducerID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.ProducerID` | `string` |

#### `producer.messagePublished`

Fires when a message is published to a queue or via an exchange.

```go
sub, _ := producerEvents.SubscribeMessagePublished(func(p producerEvents.MessagePublishedPayload) {
    fmt.Printf("Message %s published to %s by %s\n",
        p.MessageID, p.Queue.Name(), p.ProducerID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ProducerID` | `string` |

### Consumer Events

#### `consumer.up`, `consumer.down`, `consumer.goingUp`, `consumer.goingDown`

Fires when a consumer starts, stops, or begins a lifecycle transition.

```go
sub, _ := consumerEvents.SubscribeUp(func(p consumerEvents.LifecyclePayload) {
    fmt.Println("Consumer started:", p.ConsumerID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.ConsumerID` | `string` |

#### `consumer.messageReceived`

Fires when a message is dequeued and about to be processed.

```go
sub, _ := consumerEvents.SubscribeMessageReceived(func(p consumerEvents.MessageReceivedPayload) {
    fmt.Println("Received message:", p.MessageID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |

#### `consumer.messageAcknowledged`

Fires when a message is successfully processed.

```go
sub, _ := consumerEvents.SubscribeMessageAcknowledged(func(p consumerEvents.MessagePayload) {
    fmt.Println("Acknowledged:", p.MessageID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |

#### `consumer.messageUnacknowledged`

Fires when processing fails and the message is unacknowledged.

```go
sub, _ := consumerEvents.SubscribeMessageUnacknowledged(func(p consumerEvents.MessageUnacknowledgedPayload) {
    fmt.Printf("Unacknowledged %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |
| `p.Cause` | `int` |

The `cause` field is one of the [Unacknowledgment Cause](#unacknowledgment-cause) values.

#### `consumer.messageDeadLettered`

Fires when a message is moved to the dead‑letter queue.

```go
sub, _ := consumerEvents.SubscribeMessageDeadLettered(func(p consumerEvents.MessageDeadLetteredPayload) {
    fmt.Printf("Dead‑lettered %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |
| `p.Cause` | `int` |

The `cause` field is one of the [Dead‑Letter Cause](#dead‑letter-cause) values.

#### `consumer.messageRequeued`

Fires when a message is requeued for immediate retry.

```go
sub, _ := consumerEvents.SubscribeMessageRequeued(func(p consumerEvents.MessagePayload) {
    fmt.Println("Requeued:", p.MessageID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |

#### `consumer.messageDelayed`

Fires when a message is delayed for a scheduled retry.

```go
sub, _ := consumerEvents.SubscribeMessageDelayed(func(p consumerEvents.MessagePayload) {
    fmt.Println("Delayed:", p.MessageID)
})
```

Payload:

| Field | Type |
|-------|------|
| `p.MessageID` | `string` |
| `p.Queue` | `q.QueueParams` |
| `p.ConsumerID` | `string` |

## Cause Enums

### Unacknowledgment Cause

Used by `consumer.messageUnacknowledged`.

| Value | Name | Description |
|------:|------|-------------|
| 0 | `TIMEOUT` | The message consume timeout was exceeded. |
| 1 | `CONSUME_ERROR` | The handler returned an error. |
| 2 | `UNACKNOWLEDGED` | The message was explicitly unacknowledged. |
| 3 | `OFFLINE_CONSUMER` | The consumer was detected offline and its in‑flight messages were recovered. |
| 4 | `SHUTTING_DOWN` | The consumer shut down while processing the message. |
| 5 | `TTL_EXPIRED` | The message TTL expired before or during processing. |
| 6 | `QUEUE_STOPPED` | The queue was stopped while the message was in flight. |
| 7 | `QUEUE_INVALID_STATE` | The queue was in an invalid state. |
| 8 | `QUEUE_LOCKED` | The queue was locked and the operation was rejected. |
| 9 | `MESSAGE_NOT_FOUND` | The message no longer exists. |
| 10 | `QUEUE_STATE_CHANGED` | The queue state changed during processing. |
| 11 | `QUEUE_NOT_FOUND` | The queue no longer exists. |
| 12 | `UNEXPECTED_ERROR` | An unexpected internal error occurred. |
| 13 | `INVALID_HANDLER_SIGNATURE` | The message handler signature was invalid. |

### Dead‑Letter Cause

Used by `consumer.messageDeadLettered`.

| Value | Name | Description |
|------:|------|-------------|
| 0 | `TTL_EXPIRED` | The message TTL expired. |
| 1 | `RETRY_THRESHOLD_EXCEEDED` | The message exceeded its retry threshold. |
| 2 | `PERIODIC_MESSAGE` | The message was periodic (CRON/repeat) and was not retried. |

## Unsubscribing

Always call `Unsubscribe()` when you no longer need an event. The subscription object is safe to call multiple times.

```go
sub, err := queueEvents.SubscribeCreated(handler)
if err != nil {
    // handle error
}
defer sub.Unsubscribe()
```

## Internal Events

RedisSMQ also uses an internal system bus for cross‑instance synchronisation. These events are **not exposed** to external users and should not be subscribed to. The public event bus is the stable API for monitoring and integration.

## Best Practices

- Keep handlers fast; they run synchronously.
- Do not rely on events for critical data delivery; use message audit for persistent records.
- Unsubscribe when subscriptions are no longer needed.
- If you don’t need public events, you can ignore them entirely; the public bus is only started when you subscribe.
