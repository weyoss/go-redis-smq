# Namespaces

Namespaces isolate queues and exchanges. Use them to separate environments or applications within the same Redis
instance.

## Default Namespace

The default namespace comes from [configuration](configuration.md). When a queue operation uses an empty namespace, the
default is applied:

```go
// Uses default namespace from config
params := q.MustQueueParams("orders")
```

## Explicit Namespace

```go
// Uses "production" namespace regardless of default
params := q.MustQueueParamsWithNS("orders", "production")
```

## Listing Namespaces

```go
import "github.com/weyoss/go-redis-smq/pkg/namespace"

nm := namespace.NewManager()
namespaces, err := nm.List(ctx)
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
exchanges, err := nm.ListExchanges(ctx, "production")
```

## Check if a Namespace Exists

```go
exists, err := nm.Exists(ctx, "production")
```

## Valid Names

Namespace names follow the same rules as queue names:

- Start with a letter (a–z)
- Lowercase only
- Letters, digits, hyphens, underscores, dots

## Related

- [Configuration](configuration.md) — Setting the default namespace
- [Queue Management](queue-management.md) — Creating queues with namespaces