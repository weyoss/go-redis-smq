# Event Bus

RedisSMQ exposes a public event bus that lets you observe system activity in real time. Events are delivered over Redis Pub/Sub and are intended for monitoring, alerting, and integration.

The public event bus is separate from the internal system bus used for cross‑instance synchronisation. Public events are safe to use from outside the RedisSMQ package.

## Quick Start

The public event bus is not started automatically. Call `redissmq.InitUserEventBus(ctx)` before subscribing. Then use the subscription functions provided by the domain packages.

```go
import (
    "context"
    "log"

    goredis "github.com/redis/go-redis/v9"
    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

func main() {
    ctx := context.Background()

    rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
    defer rdb.Close()

    if err := redissmq.Init(ctx, rdb); err != nil {
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

| Package         | Events                                     |
|-----------------|--------------------------------------------|
| `pkg/queue`     | Queue lifecycle and state changes          |
| `pkg/consumer`  | Consumer lifecycle and message processing  |
| `pkg/producer`  | Producer lifecycle and message publication |
| `pkg/scheduler` | Scheduled-message fire-time events         |

All subscription functions return a `publiceventbus.Subscription`. Call `Unsubscribe()` when you no longer need events.

## Event Taxonomy

Messages travel through the system in two flows, and the public events reflect that split:

| Flow      | Produce-time event          | Fire-time event(s)                                                          |
|-----------|-----------------------------|-----------------------------------------------------------------------------|
| Immediate | `producer.messagePublished` | *(none — the message is pending as soon as it is produced)*                 |
| Scheduled | `producer.messageScheduled` | `scheduler.messagePublished` **or** `scheduler.messageDeadLettered`         |

The **producer** events fire when `Produce` succeeds. One event per destination queue, multiplied by the number of consumer groups for Pub/Sub destinations — matching the number of IDs `Produce` returns.

The **scheduler** events fire later, whenever the scheduled message becomes due. That may be milliseconds after produce, hours later, or never (if the message is cancelled, deleted, or the queue is dropped before its due time).

> ⚠️ **Breaking change.** `producer.messagePublished` no longer fires for scheduled messages becoming pending. Subscribers that relied on that signal (the pre-change scheduled publisher fired `producer.messagePublished` with an empty `producerID`) should migrate to `scheduler.SubscribeMessagePublished`. No other event's semantics changed.

### Full lifecycle of a scheduled message

For a repeating scheduled message with `repeat = 2`, the event sequence is:

```
producer.messageScheduled         ← Produce returns
  ↓  (repeat tick 1: clone published)
scheduler.messagePublished        ← clone's ID
  ↓  (repeat tick 2: clone published)
scheduler.messagePublished        ← another clone's ID
  ↓  (repeat limit reached; parent's cycle ends)
  (no more events — nothing to deliver)
```

For a simple (non-repeating) scheduled message that becomes due:

```
producer.messageScheduled         ← Produce returns
  ↓  (delay elapses)
scheduler.messagePublished        ← the message's own ID
  ↓  (consumer processes)
consumer.messageReceived
consumer.messageAcknowledged      ← or Unacknowledged / DeadLettered / etc.
```

If the target consumer group on a Pub/Sub queue is deleted before the message's due time, the fire-time event is `scheduler.messageDeadLettered` instead of `scheduler.messagePublished`.

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

| Field          | Type           | Description                                   |
|----------------|----------------|-----------------------------------------------|
| `p.Queue`      | `queue.Params` | The queue that was created                    |
| `p.Properties` | `queue.Props`  | Queue properties (type, delivery model, etc.) |

#### `queue.queueDeleted`

Fires when a queue is deleted.

```go
sub, _ := queue.SubscribeDeleted(func(p queue.DeletedPayload) {
    fmt.Println("Deleted queue:", p.Queue.Name())
})
```

Payload:

| Field     | Type           |
|-----------|----------------|
| `p.Queue` | `queue.Params` |

#### `queue.stateChanged`

Fires when the operational state of a queue changes.

```go
sub, _ := queue.SubscribeStateChanged(func(p queue.StateChangedPayload) {
    fmt.Printf("Queue %s changed from %v to %v\n",
        p.Queue.Name(), p.Transition.From, p.Transition.To)
})
```

Payload:

| Field          | Type                     |
|----------------|--------------------------|
| `p.Queue`      | `queue.Params`           |
| `p.Transition` | `queue.StateTransition`  |

#### `queue.consumerGroupCreated`

Fires when a consumer group is created for a Pub/Sub queue.

```go
sub, _ := queue.SubscribeConsumerGroupCreated(func(p queue.ConsumerGroupCreatedPayload) {
    fmt.Println("Group:", p.GroupID)
})
```

Payload:

| Field       | Type           |
|-------------|----------------|
| `p.Queue`   | `queue.Params` |
| `p.GroupID` | `string`       |

#### `queue.consumerGroupDeleted`

Fires when a consumer group is deleted.

```go
sub, _ := queue.SubscribeConsumerGroupDeleted(func(p queue.ConsumerGroupDeletedPayload) {
    fmt.Println("Deleted group:", p.GroupID)
})
```

Payload:

| Field       | Type           |
|-------------|----------------|
| `p.Queue`   | `queue.Params` |
| `p.GroupID` | `string`       |

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

Fires when an **immediate** (non-scheduled) produce succeeds — one event per destination queue, multiplied by the number of consumer groups for Pub/Sub destinations.

Does **not** fire for scheduled produces; see `producer.messageScheduled` below.

```go
sub, _ := producer.SubscribeMessagePublished(func(p producer.MessagePublishedPayload) {
    fmt.Printf("Message %s published to %s by %s\n",
        p.MessageID, p.Queue.QueueParams.Name(), p.ProducerID)
})
```

Payload:

| Field          | Type                 | Description                                                                          |
|----------------|----------------------|--------------------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                                           |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` (name, ns) + `GroupID` (empty for point-to-point)                      |
| `p.ProducerID` | `string`             | Producer ID                                                                          |

#### `producer.messageScheduled`

Fires when a **scheduled** produce succeeds — one event per destination queue, multiplied by the number of consumer groups for Pub/Sub destinations. The message has entered the scheduled set; its due time is set (delay, CRON, repeat) but not yet reached.

Does **not** fire for immediate produces; see `producer.messagePublished` above.

```go
sub, _ := producer.SubscribeMessageScheduled(func(p producer.MessageScheduledPayload) {
    fmt.Printf("Message %s scheduled on %s by %s\n",
        p.MessageID, p.Queue.QueueParams.Name(), p.ProducerID)
})
```

Payload:

| Field          | Type                 | Description                                                                          |
|----------------|----------------------|--------------------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                                           |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` (name, ns) + `GroupID` (empty for point-to-point)                      |
| `p.ProducerID` | `string`             | Producer ID                                                                          |

For a repeating message, this fires once — for the parent. The clones created by subsequent repeat ticks each fire `scheduler.messagePublished` (see below), not `producer.messageScheduled`.

### Scheduler Events

Scheduler events fire when a scheduled message becomes due. They do **not** carry a `ProducerID` — the scheduler is not a producer, and by the time a message's due time arrives the originating producer may have long since shut down.

#### `scheduler.messagePublished`

Fires when the scheduled publisher moves a due scheduled message into a pending list, ready for consumption. One event per published ID.

For a simple (non-repeating) scheduled message, `MessageID` is the message's own ID (as returned by the original `Produce` call).

For a repeating message, `MessageID` is the **clone's** ID — the parent stays in the scheduled set for its next tick, so `scheduler.messagePublished` fires once per clone, not for the parent.

```go
import "github.com/weyoss/go-redis-smq/pkg/scheduler"

sub, _ := scheduler.SubscribeMessagePublished(func(p scheduler.MessagePublishedPayload) {
    fmt.Printf("Message %s now pending on %s\n",
        p.MessageID, p.Queue.QueueParams.Name())
})
```

Payload:

| Field         | Type                 | Description                                                              |
|---------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID` | `string`             | Message ID (simple) or clone ID (repeating)                              |
| `p.Queue`     | `queue.ParsedParams` | `QueueParams` (name, ns) + `GroupID` (empty for point-to-point)          |

#### `scheduler.messageDeadLettered`

Fires when a due scheduled message could not be delivered because its target consumer group no longer exists.

On a Pub/Sub queue, a message is bound to a specific consumer group at produce time. If that group is deleted between produce and the message's due time, there is nowhere for the message to go — the scheduled publisher dead-letters it instead of orphaning it.

For a **simple** scheduled message, the message itself is dead-lettered and `MessageID` is the message's own ID.

For a **repeating** scheduled message, the parent is dead-lettered to terminate its cycle; the would-be clone for the current firing is discarded. `MessageID` is the parent's ID.

```go
sub, _ := scheduler.SubscribeMessageDeadLettered(func(p scheduler.MessageDeadLetteredPayload) {
    fmt.Printf("Message %s dead-lettered on %s (group %q gone)\n",
        p.MessageID, p.Queue.QueueParams.Name(), p.Queue.GroupID)
})
```

Payload:

| Field         | Type                 | Description                                                              |
|---------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID` | `string`             | Message ID (simple) or parent ID (repeating)                             |
| `p.Queue`     | `queue.ParsedParams` | `QueueParams` (name, ns) + `GroupID` (the deleted group's ID)            |

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

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID` (empty for point-to-point)           |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |

#### `consumer.messageAcknowledged`

Fires when a message is successfully processed.

```go
sub, _ := consumer.SubscribeMessageAcknowledged(func(p consumer.MessagePayload) {
    fmt.Println("Acknowledged:", p.MessageID)
})
```

Payload:

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID`                                      |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |

#### `consumer.messageUnacknowledged`

Fires when processing fails and the message is unacknowledged.

```go
sub, _ := consumer.SubscribeMessageUnacknowledged(func(p consumer.MessageUnacknowledgedPayload) {
    fmt.Printf("Unacknowledged %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID`                                      |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |
| `p.Cause`      | `int`                | A `consumer.UnacknowledgeCause` value (see `unacknowledge_cause.go`)     |

#### `consumer.messageDeadLettered`

Fires when a message is moved to the dead‑letter queue.

```go
sub, _ := consumer.SubscribeMessageDeadLettered(func(p consumer.MessageDeadLetteredPayload) {
    fmt.Printf("Dead‑lettered %s, cause: %d\n", p.MessageID, p.Cause)
})
```

Payload:

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID`                                      |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |
| `p.Cause`      | `int`                | A `consumer.DeadLetterCause` value (see `deadletter_cause.go`)           |

#### `consumer.messageRequeued`

Fires when a message is requeued for immediate retry.

```go
sub, _ := consumer.SubscribeMessageRequeued(func(p consumer.MessagePayload) {
    fmt.Println("Requeued:", p.MessageID)
})
```

Payload:

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID`                                      |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |

#### `consumer.messageDelayed`

Fires when a message is delayed for a scheduled retry.

```go
sub, _ := consumer.SubscribeMessageDelayed(func(p consumer.MessagePayload) {
    fmt.Println("Delayed:", p.MessageID)
})
```

Payload:

| Field          | Type                 | Description                                                              |
|----------------|----------------------|--------------------------------------------------------------------------|
| `p.MessageID`  | `string`             | Message ID                                                               |
| `p.Queue`      | `queue.ParsedParams` | `QueueParams` + effective `GroupID`                                      |
| `p.ConsumerID` | `string`             | Consumer ID                                                              |

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
- If you don't need public events, you can ignore them entirely; the public bus is only started when you subscribe.
- When tracking a scheduled message end-to-end, subscribe to **both** `producer.SubscribeMessageScheduled` (produce-time) and `scheduler.SubscribeMessagePublished` / `SubscribeMessageDeadLettered` (fire-time). Neither package alone gives the full lifecycle.
