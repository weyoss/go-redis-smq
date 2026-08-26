# Exchange Management

Create and manage exchanges for message routing. Direct, topic, and fanout exchanges are supported.

All exchange implementations are created via factory functions in `redissmq`. The public `exchange` package contains only interfaces and types.

## Obtain Exchange Managers

```go
import (
    "github.com/weyoss/go-redis-smq"
    "github.com/weyoss/go-redis-smq/pkg/exchange"
)

em := redissmq.NewExchangeManager()      // returns exchange.Manager
dx := redissmq.NewDirectExchange()       // returns exchange.DirectExchange
fx := redissmq.NewFanoutExchange()       // returns exchange.FanoutExchange
tx := redissmq.NewTopicExchange()        // returns exchange.TopicExchange
```

## Direct Exchange

Routes messages by exact routing key match.

```go
dx := redissmq.NewDirectExchange()

// Create a direct exchange
params := exchange.MustExchangeParams("orders", exchange.TypeDirect)
err := dx.Create(ctx, params, exchange.PolicyStandard)

// Bind a queue
err = dx.BindQueue(ctx, queueParams, params, "order.created")

// Match queues for a routing key
queues, err := dx.MatchQueues(ctx, params, "order.created")

// List routing keys
keys, err := dx.RoutingKeys(ctx, params)

// List bindings
bindings, err := dx.Bindings(ctx, params)

// Unbind
err = dx.UnbindQueue(ctx, queueParams, params, "order.created")

// Delete
err = dx.Delete(ctx, params)
```

## Topic Exchange

Routes messages by pattern matching with wildcards (`*` and `#`).

```go
tx := redissmq.NewTopicExchange()

// Create
params := exchange.MustExchangeParams("events", exchange.TypeTopic)
err := tx.Create(ctx, params, exchange.PolicyStandard)

// Bind with pattern
err = tx.BindQueue(ctx, queueParams, params, "user.*")
err = tx.BindQueue(ctx, queueParams, params, "order.#")

// Match queues for a routing key
queues, err := tx.MatchQueues(ctx, params, "user.login.success")

// List patterns
patterns, err := tx.Patterns(ctx, params)

// Delete
err = tx.Delete(ctx, params)
```

## Fanout Exchange

Broadcasts to all bound queues.

```go
fx := redissmq.NewFanoutExchange()

// Create
params := exchange.MustExchangeParams("alerts", exchange.TypeFanout)
err := fx.Create(ctx, params, exchange.PolicyStandard)

// Bind queues
err = fx.BindQueue(ctx, emailQueue, params)
err = fx.BindQueue(ctx, smsQueue, params)

// All bound queues receive every message
queues, err := fx.MatchQueues(ctx, params)

// Delete
err = fx.Delete(ctx, params)
```

## Exchange Policies

| Policy                    | Allowed Queue Types  |
|---------------------------|----------------------|
| `exchange.PolicyStandard` | FIFO, LIFO           |
| `exchange.PolicyPriority` | Priority only        |

## Namespace Validation

Queues and exchanges must be in the same namespace. Binding across namespaces returns an error.

## Discovery

Use the exchange manager for discovery operations:

```go
em := redissmq.NewExchangeManager()

// All exchanges
all, err := em.ListAll(ctx)

// By namespace
byNS, err := em.ListByNamespace(ctx, "production")

// Exchanges bound to a queue
byQueue, err := em.ListByQueue(ctx, queueParams)
```

## Related

- [Message Exchanges](https://github.com/weyoss/redis-smq-docs) — Exchange concepts
- [Producing Messages](producing-messages.md) — Sending via exchanges
