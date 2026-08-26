# Event Bus

RedisSMQ exposes a public event bus that lets you observe system activity in real time. Events are delivered over Redis Pub/Sub and are intended for monitoring, alerting, and integration.

The public event bus is separate from the internal system bus used for cross‑instance synchronisation. Public events are safe to use from outside the RedisSMQ package.

## Quick Start

The public event bus is not started automatically. Call `redissmq.InitUserEventBus(ctx)` before subscribing. Then use the subscription functions provided by the domain packages.

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
    ctx := context.Background()
    if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
        log.Fatal(err)
    }
    defer redissmq.Shutdown()

    redissmq.InitUserEventBus(ctx)

    sub, err := queue.SubscribeCreated(func(p queue.CreatedPayload) {
        log.Printf("Queue created: %s", p.Queue.Name())
    })
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()
}
```

## Subscribing to Events

Subscription functions are exposed directly in the domain packages:

| Package        | Events                                     |
|----------------|--------------------------------------------|
| `pkg/queue`    | Queue lifecycle and state changes          |
| `pkg/consumer` | Consumer lifecycle and message processing  |
| `pkg/producer` | Producer lifecycle and message publication |

All subscription functions return a `publiceventbus.Subscription`. Call `Unsubscribe()` when you no longer need events.

## Available Events

### Queue Events

#### `queue.queueCreated`

Fires when a new queue is created.

```go
sub, _ := queue.SubscribeCreated(func(p queue.CreatedPayload) {
    fmt.Println("Queue:", p.Queue.Name())
})
```

Payload:

| Field          | Type                | Description                                   |
|----------------|---------------------|-----------------------------------------------|
| `p.Queue`      | `queue.QueueParams` | The queue that was created                    |
| `p.Properties` | `queue.QueueProps`  | Queue properties (type, delivery model, etc.) |

#### `queue.queueDeleted`

Fires when a queue is deleted.

```go
sub, _ := queue.SubscribeDeleted(func(p queue.DeletedPayload) {
    fmt.Println("Deleted queue:", p.Queue.Name())
})
```

Payload:

| Field     | Type                |
|-----------|---------------------|
| `p.Queue` | `queue.QueueParams` |

#### `queue.stateChanged`

Fires when the operational state of a queue changes.

```go
sub, _ := queue.SubscribeStateChanged(func(p queue.StateChangedPayload) {
    fmt.Printf("Queue %s changed from %v to %v\n",
        p.Queue.Name(), p.Transition.From, p.Transition.To)
})
```

Payload:

| Field          | Type                    |
|----------------|-------------------------|
| `p.Queue`      | `queue.QueueParams`     |
| `p.Transition` | `queue.StateTransition` |

#### `queue.consumerGroupCreated`

Fires when a consumer group is created for a Pub/Sub queue.

```go
sub, _ := queue.SubscribeConsumerGroupCreated(func(p queue.ConsumerGroupCreatedPayload) {
    fmt.Println("Group:", p.GroupID)
})
```

Payload:

| Field       | Type                |
|-------------|---------------------|
| `p.Queue`   | `queue.QueueParams` |
| `p.GroupID` | `string`            |

#### `queue.consumerGroupDeleted`

Fires when a consumer group is deleted.

```go
sub, _ := queue.SubscribeConsumerGroupDeleted(func(p queue.ConsumerGroupDeletedPayload) {
    fmt.Println("Deleted group:", p.GroupID)
})
```

Payload:

| Field       | Type                |
|-------------|---------------------|
| `p.Queue`   | `queue.QueueParams` |
| `p.GroupID` | `string`            |

### Producer Events

#### `producer.up`, `producer.down`, `producer.goingUp`, `producer.goingDown`

Fires when a producer starts, stops, or begins a lifecycle transition.

```go
sub, _ := producer.SubscribeUp(func(p producer.LifecyclePayload) {
    fmt.Println("Producer started:", p.ProducerID)
})
```

Payload:

| Field          | Type     |
|----------------|----------|
| `p.ProducerID` | `string` |

#### `producer.messagePublished`

Fires when a message is published to a queue or via an exchange.

```go
sub, _ := producer.SubscribeMessagePublished(func(p producer.MessagePublishedPayload) {
    fmt.Printf("Message %s published to %s by %s\n",
        p.MessageID, p.Queue.Name(), p.ProducerID)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ProducerID` | `string`            |

### Consumer Events

#### `consumer.up`, `consumer.down`, `consumer.goingUp`, `consumer.goingDown`

Fires when a consumer starts, stops, or begins a lifecycle transition.

```go
sub, _ := consumer.SubscribeUp(func(p consumer.LifecyclePayload) {
    fmt.Println("Consumer started:", p.ConsumerID)
})
```

Payload:

| Field          | Type     |
|----------------|----------|
| `p.ConsumerID` | `string` |

#### `consumer.messageReceived`

Fires when a message is dequeued and about to be processed.

```go
sub, _ := consumer.SubscribeMessageReceived(func(p consumer.MessageReceivedPayload) {
    fmt.Println("Received message:", p.MessageID)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |

#### `consumer.messageAcknowledged`

Fires when a message is successfully processed.

```go
sub, _ := consumer.SubscribeMessageAcknowledged(func(p consumer.MessagePayload) {
    fmt.Println("Acknowledged:", p.MessageID)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |

#### `consumer.messageUnacknowledged`

Fires when processing fails and the message is unacknowledged.

```go
sub, _ := consumer.SubscribeMessageUnacknowledged(func(p consumer.MessageUnacknowledgedPayload) {
    fmt.Printf("Unacknowledged %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |
| `p.Cause`      | `int`               |

#### `consumer.messageDeadLettered`

Fires when a message is moved to the dead‑letter queue.

```go
sub, _ := consumer.SubscribeMessageDeadLettered(func(p consumer.MessageDeadLetteredPayload) {
    fmt.Printf("Dead‑lettered %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |
| `p.Cause`      | `int`               |

#### `consumer.messageRequeued`

Fires when a message is requeued for immediate retry.

```go
sub, _ := consumer.SubscribeMessageRequeued(func(p consumer.MessagePayload) {
    fmt.Println("Requeued:", p.MessageID)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |

#### `consumer.messageDelayed`

Fires when a message is delayed for a scheduled retry.

```go
sub, _ := consumer.SubscribeMessageDelayed(func(p consumer.MessagePayload) {
    fmt.Println("Delayed:", p.MessageID)
})
```

Payload:

| Field          | Type                |
|----------------|---------------------|
| `p.MessageID`  | `string`            |
| `p.Queue`      | `queue.QueueParams` |
| `p.ConsumerID` | `string`            |

## Unsubscribing

Always call `Unsubscribe()` when you no longer need an event. The subscription object is safe to call multiple times.

```go
sub, err := queue.SubscribeCreated(handler)
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
