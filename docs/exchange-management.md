# Exchange Management

Create and manage exchanges for message routing. Direct, topic, and fanout exchanges are supported.

## Direct Exchange

Routes messages by exact routing key match.

```go
import (
"github.com/weyoss/go-redis-smq/pkg/exchange"
"github.com/weyoss/go-redis-smq/pkg/exchange/x"
)

dx := exchange.NewDirectExchange(nil)

// Create
params := x.NewExchangeParams("orders", x.TypeDirect)
err := dx.Create(ctx, params, x.PolicyStandard)

// Bind a queue
err = dx.BindQueue(ctx, queueParams, exchangeParams, "order.created")

// Match queues for a routing key
queues, err := dx.MatchQueues(ctx, exchangeParams, "order.created")

// List routing keys
keys, err := dx.RoutingKeys(ctx, exchangeParams)

// List bindings
bindings, err := dx.Bindings(ctx, exchangeParams)

// Unbind
err = dx.UnbindQueue(ctx, queueParams, exchangeParams, "order.created")

// Delete
err = dx.Delete(ctx, exchangeParams)
```

## Topic Exchange

Routes messages by pattern matching with wildcards (`*` and `#`).

```go
tx := exchange.NewTopicExchange(nil)

// Create
params := x.NewExchangeParams("events", x.TypeTopic)
err := tx.Create(ctx, params, x.PolicyStandard)

// Bind with pattern
err = tx.BindQueue(ctx, queueParams, exchangeParams, "user.*")
err = tx.BindQueue(ctx, queueParams, exchangeParams, "order.#")

// Match queues for a routing key
queues, err := tx.MatchQueues(ctx, exchangeParams, "user.login.success")

// List patterns
patterns, err := tx.Patterns(ctx, exchangeParams)

// Delete
err = tx.Delete(ctx, exchangeParams)
```

## Fanout Exchange

Broadcasts to all bound queues.

```go
fx := exchange.NewFanoutExchange(nil)

// Create
params := x.NewExchangeParams("alerts", x.TypeFanout)
err := fx.Create(ctx, params, x.PolicyStandard)

// Bind queues
err = fx.BindQueue(ctx, emailQueue, exchangeParams)
err = fx.BindQueue(ctx, smsQueue, exchangeParams)

// All bound queues receive every message
queues, err := fx.MatchQueues(ctx, exchangeParams)

// Delete
err = fx.Delete(ctx, exchangeParams)
```

## Exchange Policies

| Policy             | Allowed Queue Types |
|--------------------|---------------------|
| `x.PolicyStandard` | FIFO, LIFO          |
| `x.PolicyPriority` | Priority only       |

## Namespace Validation

Queues and exchanges must be in the same namespace. Binding across namespaces returns an error.

## Discovery

```go
em := exchange.NewManager()

// All exchanges
all, err := em.ListAll(ctx)

// By namespace
byNS, err := em.ListByNamespace(ctx, "production")

// Exchanges bound to a queue
byQueue, err := em.ListByQueue(ctx, queueParams)
```

## Related

- [Message Exchanges](../../../docs/message-exchanges.md) — Exchange concepts
- [Producing Messages](producing-messages.md) — Sending via exchanges