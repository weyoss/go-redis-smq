# Message Management

Retrieve, delete, and requeue messages by ID.

## Obtain the Message Manager

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
)

mm := redissmq.NewMessageManager()
```

The manager is created via the root `redissmq` package and implements the public `message.Manager` interface.

## Retrieve Messages

```go
import "github.com/weyoss/go-redis-smq/pkg/message"

// Single message
m, err := mm.Get(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Body:", m.Body)
fmt.Println("Status:", m.Status)

// Multiple messages
msgs, err := mm.GetAll(ctx, []string{"msg-1", "msg-2"})
if err != nil {
    log.Fatal(err)
}
for _, m := range msgs {
    fmt.Println(m.ID, m.Status)
}
```

## Inspect Messages

```go
status, err := mm.Status(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Status:", status)

state, err := mm.State(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Attempts:", state.Attempts)
fmt.Println("Expired:", state.Expired)
```

## Delete Messages

```go
// Single
result, err := mm.Delete(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Deleted:", result.Stats.Success)

// Multiple
result, err = mm.DeleteAll(ctx, []string{"msg-1", "msg-2"})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Deleted: %d/%d\n", result.Stats.Success, result.Stats.Processed)
```

## Requeue Messages

Requeue creates a copy of an acknowledged or dead-lettered message:

```go
newID, err := mm.Requeue(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Requeued as:", newID)
```

Only acknowledged and dead-lettered messages can be requeued. The original is unchanged.

## Unacknowledgment History

Requires `MessageAudit.UnacknowledgementHistory` to be enabled:

```go
history, err := mm.UnacknowledgmentHistory(ctx, "msg-123")
if err != nil {
    log.Fatal(err)
}
for _, record := range history {
    fmt.Println(record)
}
```

## Related

- [Message Browsing](message-browsing.md) — Browse queue messages
- [Messages](https://github.com/weyoss/redis-smq-docs) — Message lifecycle
