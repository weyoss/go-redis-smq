# Namespaces

Namespaces isolate queues and exchanges. Use them to separate environments or applications within the same Redis instance.

## Obtain the Namespace Manager

```go
import (
    "context"
    "log"

    "github.com/weyoss/go-redis-smq"
)

nm := redissmq.NewNamespaceManager()
```

The manager is created via the root `redissmq` package and implements the public `namespace.Manager` interface.

## Default Namespace

The default namespace comes from [configuration](configuration.md). When a queue or exchange operation uses an empty namespace, the default is applied automatically by the constructors in `pkg/queue` and `pkg/exchange`:

```go
import "github.com/weyoss/go-redis-smq/pkg/queue"

// Uses default namespace from configuration
params := queue.MustQueueParams("orders")
```

## Explicit Namespace

```go
// Uses "production" namespace regardless of default
params := queue.MustQueueParamsWithNS("orders", "production")
```

## Listing Namespaces

```go
namespaces, err := nm.List(ctx)
if err != nil {
    log.Fatal(err)
}
// ["production", "staging", "analytics"]
```

## Delete a Namespace

```go
err := nm.Delete(ctx, "staging")
```

Deleting a namespace removes all queues and exchanges within it.

## List Queues and Exchanges in a Namespace

```go
queues, err := nm.ListQueues(ctx, "production")
if err != nil {
    log.Fatal(err)
}

exchanges, err := nm.ListExchanges(ctx, "production")
if err != nil {
    log.Fatal(err)
}
```

## Check if a Namespace Exists

```go
exists, err := nm.Exists(ctx, "production")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Exists:", exists)
```

## Valid Names

Namespace names follow the same rules as queue names:

- Start with a letter (a–z)
- Lowercase only
- Letters, digits, hyphens, underscores, dots

## Related

- [Configuration](configuration.md) — Setting the default namespace
- [Queue Management](queue-management.md) — Creating queues with namespaces
