# Message Management

Retrieve, delete, and requeue messages by ID.

## Retrieve Messages

```go
import (
"github.com/weyoss/go-redis-smq/pkg/message"
"github.com/weyoss/go-redis-smq/pkg/message/msg"
)

// Single message
m, err := message.Get(ctx, "msg-123")
fmt.Println("Body:", m.Body)
fmt.Println("Status:", m.Status)

// Multiple messages
msgs, err := message.GetAll(ctx, []string{"msg-1", "msg-2"})
for _, m := range msgs {
fmt.Println(m.ID, m.Status)
}
```

## Inspect Messages

```go
status, err := message.Status(ctx, "msg-123")
state, err := message.State(ctx, "msg-123")
fmt.Println("Attempts:", state.Attempts)
fmt.Println("Expired:", state.Expired)
```

## Delete Messages

```go
// Single
result, err := message.Delete(ctx, "msg-123")
fmt.Println("Deleted:", result.Stats.Success)

// Multiple
result, err := message.DeleteAll(ctx, []string{"msg-1", "msg-2"})
fmt.Printf("Deleted: %d/%d\n", result.Stats.Success, result.Stats.Processed)
```

## Requeue Messages

Requeue creates a copy of an acknowledged or dead-lettered message:

```go
newID, err := message.Requeue(ctx, "msg-123")
fmt.Println("Requeued as:", newID)
```

Only acknowledged and dead-lettered messages can be requeued. The original is unchanged.

## Unacknowledgment History

Requires `MessageAudit.UnacknowledgementHistory` to be enabled:

```go
history, err := message.UnacknowledgmentHistory(ctx, "msg-123")
for _, record := range history {
fmt.Println(record)
}
```

## Related

- [Message Browsing](message-browsing.md) — Browse queue messages
- [Messages](../../../docs/messages.md) — Message lifecycle