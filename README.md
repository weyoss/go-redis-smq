<div align="center" style="text-align: center">
  <p>
    <a href="https://github.com/weyoss/go-redis-smq">
      <img src="logo.png" alt="RedisSMQ" width="500px" />
    </a>
  </p>
  <p><strong>High‑performance Redis message queue for Go</strong><br />simple to use, built for scale.</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/weyoss/go-redis-smq.svg)](https://pkg.go.dev/github.com/weyoss/go-redis-smq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)
</div>

---

**Other implementations:** [redis-smq](https://github.com/weyoss/redis-smq) (TypeScript)  
**Language‑agnostic concepts:** [redis-smq-docs](https://github.com/weyoss/redis-smq-docs) – architecture, queues, exchanges, and more.

## ✨ Why RedisSMQ?

- **Full‑featured** – FIFO, LIFO, priority queues, pub/sub, exchanges, scheduling, consumer groups, rate limiting.
- **Reliable** – Acknowledgements, dead‑letter queues, retries, and message persistence.
- **Cross‑language** – Messages published from Go can be consumed by Node.js (and vice versa).
- **Go‑native** – Idiomatic API, context support, and clean concurrency.

## 📋 Requirements

- **Go** ≥ 1.25
- **Redis** ≥ 4

> 📊 See [BUILD.md](BUILD.md) for CI status, code coverage, and build instructions.

## 📦 Installation

```bash
go get github.com/weyoss/go-redis-smq
```

## 🚀 Quick Start

### 1. Initialize

```go
package main

import (
    "context"
    "log"

	"github.com/redis/go-redis/v9"
    "github.com/weyoss/go-redis-smq"
)

func main() {
    ctx := context.Background()
    if err := redissmq.Init(ctx, redis.Options{Addr: "127.0.0.1:6379"}); err != nil {
        log.Fatal(err)
    }
    defer redissmq.Shutdown()
}
```

### 2. Create a Queue

```go
import (
    "github.com/weyoss/go-redis-smq/pkg/queue"
)

params := queue.MustQueueParams("orders")
if err := redissmq.NewQueueManager().Create(ctx, params, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
    log.Fatal(err)
}
```

### 3. Produce a Message

```go
import (
    "github.com/weyoss/go-redis-smq/pkg/message"
)

producer := redissmq.NewProducer()
if err := producer.Run(ctx); err != nil {
    log.Fatal(err)
}
defer producer.Shutdown(ctx)

m := message.New().SetBody("Hello World").SetQueue(params)
ids, err := producer.Produce(ctx, m)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Produced: %v\n", ids)
```

### 4. Consume Messages

```go
consumer := redissmq.NewConsumer()
consumer.Consume(params, func(ctx context.Context, m *message.Transferable) error {
    fmt.Printf("Received: %v\n", string(m.Body.(string)))
    return nil
})
if err := consumer.Run(ctx); err != nil {
    log.Fatal(err)
}
defer consumer.Shutdown()
```

## 🏗️ Architecture

RedisSMQ uses a clean, layered architecture:

- **Public packages (`pkg/...`)** contain only interfaces, types, and documentation. They never import internal code.
- **Internal packages (`internal/...`)** hold concrete Redis‑backed implementations.
- **Root `redissmq` package** is the composition root. It provides factory functions that return concrete implementations behind public interfaces. Use these factories to obtain managers, producers, consumers, exchanges, etc.

### Factory Functions

| Factory                              | Returns                      |
|--------------------------------------|------------------------------|
| `redissmq.NewQueueManager()`         | `queue.QueueManager`         |
| `redissmq.NewStateManager()`         | `queue.StateManager`         |
| `redissmq.NewConsumerGroupManager()` | `queue.ConsumerGroupManager` |
| `redissmq.NewMessageManager()`       | `message.MessageManager`     |
| `redissmq.NewExchangeManager()`      | `exchange.Manager`           |
| `redissmq.NewDirectExchange()`       | `exchange.DirectExchange`    |
| `redissmq.NewFanoutExchange()`       | `exchange.FanoutExchange`    |
| `redissmq.NewTopicExchange()`        | `exchange.TopicExchange`     |
| `redissmq.NewNamespaceManager()`     | `namespace.Manager`          |
| `redissmq.NewProducer()`             | `producer.Producer`          |
| `redissmq.NewConsumer()`             | `consumer.Consumer`          |
| `redissmq.NewConfigManager()`        | `config.Manager`             |

## 📚 API Overview

The public API is split across packages:

- **System** – `Init`, `Shutdown`
- **Queues** – `Create`, `Pause`, `Resume`, `Stop`, `SetRateLimit`, `BrowseMessages`, `ListAll`
- **Messages** – `Get`, `Delete`, `Requeue`
- **Producers** – `Run`, `Produce` (direct or via exchanges)
- **Consumers** – `Consume`, `ConsumeWithGroup`, `Run`, `Shutdown`
- **Exchanges** – Direct, Topic, Fanout with bindings
- **Namespaces** – `List`, `Delete`, `ListQueues`, `ListExchanges`
- **Configuration** – runtime config via `config.Manager`
- **Events** – public event bus for monitoring

## 🔗 Interoperability

Because the Go and TypeScript implementations share the same protocol, you can:

- Produce in Go, consume in Node.js (and vice versa).
- Manage queues and exchanges from either language.
- Use the same Redis instance for both stacks.

The REST API and Web UI (from the TypeScript repo) work seamlessly with Go‑created queues.

## 🧩 Compatibility

Always match your library version with the correct language‑agnostic specification.  
Check the [version compatibility matrix](https://github.com/weyoss/redis-smq-docs#compatibility-matrix) before upgrading.

## 📄 License

MIT – see [LICENSE](LICENSE).
