# Quick Start

Get RedisSMQ running in your Go application in minutes.

## 1. Install

```bash
go get github.com/weyoss/go-redis-smq
```

## 2. Initialize

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

## 3. Create a Queue

```go
import (
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

ordersQueue := queue.MustQueueParams("orders")
if err := redissmq.NewQueueManager().Create(ctx, ordersQueue, queue.TypeFIFO, queue.DeliveryPointToPoint); err != nil {
	log.Fatal(err)
}
```

## 4. Produce a Message

```go
import (
	"github.com/weyoss/go-redis-smq/pkg/message"
)

producer := redissmq.NewProducer()
if err := producer.Run(ctx); err != nil {
	log.Fatal(err)
}
defer producer.Shutdown(ctx)

m := message.New().SetBody("Hello World").SetQueue(ordersQueue)
ids, err := producer.Produce(ctx, m)
if err != nil {
	log.Fatal(err)
}
log.Printf("Sent: %v", ids)
```

## 5. Consume Messages

```go
consumer := redissmq.NewConsumer()
consumer.Consume(ordersQueue, func(ctx context.Context, m *message.Transferable) error {
	log.Printf("Received: %v", m.Body)
	return nil // return error to trigger retry
})

if err := consumer.Run(ctx); err != nil {
	log.Fatal(err)
}
defer consumer.Shutdown()
```

## 6. Shutdown

```go
// Shutdown is called via defer in step 2
// redissmq.Shutdown() handles producers, consumers, and connections
```

## Next Steps

- [Producing Messages](producing-messages.md) — All publish options
- [Consuming Messages](consuming-messages.md) — All consume options
- [Queue Management](queue-management.md) — Queue CRUD, state, rate limiting
- [Shared Concepts](https://github.com/weyoss/redis-smq-docs) — Language-agnostic documentation
