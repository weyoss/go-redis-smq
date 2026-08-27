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

    goredis "github.com/redis/go-redis/v9"
    "github.com/weyoss/go-redis-smq"
)

func main() {
    ctx := context.Background()

    // Create a Redis client. Cluster/ring clients are not supported.
    rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
    defer rdb.Close()

    if err := redissmq.Init(ctx, rdb); err != nil {
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
    fmt.Printf("Received: %v\n", m.Body)
    return nil
})
if err := consumer.Run(ctx); err != nil {
    log.Fatal(err)
}
defer consumer.Shutdown()
```

## 🏗️ Architecture

RedisSMQ is built around a small set of core concepts:

- **Queues** – store messages and define ordering (FIFO, LIFO, priority) and delivery model (point‑to‑point, pub/sub).
- **Exchanges** – route messages to one or more queues based on routing rules (direct, topic, fanout).
- **Producers** – publish messages to queues or exchanges.
- **Consumers** – subscribe to queues and process messages with a handler.
- **Messages** – the data units transferred through the system.
- **Namespaces** – logical isolation boundaries for queues and exchanges.
- **Configuration** – runtime settings shared across connected instances.

The `redissmq` package is the entry point: it initialises the runtime and provides factory functions for all components.

### Getting Components

Use the root package to create concrete implementations behind the public interfaces:

| Factory                              | Returns                      |
|--------------------------------------|------------------------------|
| `redissmq.NewQueueManager()`         | `queue.Manager`              |
| `redissmq.NewStateManager()`         | `queue.StateManager`         |
| `redissmq.NewConsumerGroupManager()` | `queue.ConsumerGroupManager` |
| `redissmq.NewMessageManager()`       | `message.Manager`            |
| `redissmq.NewExchangeManager()`      | `exchange.Manager`           |
| `redissmq.NewDirectExchange()`       | `exchange.DirectExchange`    |
| `redissmq.NewFanoutExchange()`       | `exchange.FanoutExchange`    |
| `redissmq.NewTopicExchange()`        | `exchange.TopicExchange`     |
| `redissmq.NewNamespaceManager()`     | `namespace.Manager`          |
| `redissmq.NewProducer()`             | `producer.Producer`          |
| `redissmq.NewConsumer()`             | `consumer.Consumer`          |
| `redissmq.NewConfigManager()`        | `config.Manager`             |

## 📚 Documentation

- [Go API Guides](docs/README.md) – quick start, queues, messages, exchanges, configuration, events, and more
- [Language‑agnostic Concepts](https://github.com/weyoss/redis-smq-docs) – architecture, reliability, delivery models

For a complete list of guides, see the [documentation index](docs/README.md).

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
