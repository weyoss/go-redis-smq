# Consumer Groups

Manage consumer groups for Pub/Sub queues. Groups enable multiple services to each receive a copy of every message.

## Create a Group

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

cgm := queue.NewConsumerGroupManager()

result, err := cgm.Save(ctx, queueParams, "email-service")
// result == 1 if created, 0 if already exists
```

Groups are only supported on Pub/Sub queues.

## Delete a Group

```go
err := cgm.Delete(ctx, queueParams, "email-service")
```

The group must be empty and have no active consumers.

## List Groups

```go
groups, err := cgm.List(ctx, queueParams)
// ["email-service", "sms-service"]
```

## Using Groups with Consumers

```go
consumer.ConsumeWithGroup(queueParams, "email-service", func (ctx context.Context, m *msg.Transferable) error {
// Only one consumer in this group gets the message
return nil
})
```

## Ephemeral Groups

If a consumer subscribes without specifying a group ID on a Pub/Sub queue, an ephemeral group is created automatically
and deleted on shutdown:

```go
consumer.Consume(queueParams, handler) // ephemeral group created
```

## Related

- [Consumer Groups Concepts](../../../docs/consumer-groups.md) — How groups work
- [Queue Delivery Models](../../../docs/queue-delivery-models.md) — Point-to-Point vs Pub/Sub
- [Consuming Messages](consuming-messages.md) — Subscribing with groups