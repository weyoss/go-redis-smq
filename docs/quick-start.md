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

	"github.com/weyoss/go-redis-smq"
)

func main() {
	ctx := context.Background()

	if err := redissmq.Init(ctx, redissmq.Config{Addr: "127.0.0.1:6379"}); err != nil {
		log.Fatal(err)
	}
	defer redissmq.Shutdown()
}
```

## 3. Create a Queue

```go
import (
"github.com/weyoss/go-redis-smq/pkg/queue"
"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

ordersQueue := q.MustQueueParams("orders")
if err := queue.Create(ctx, ordersQueue, q.TypeFIFO, q.DeliveryPointToPoint); err != nil {
log.Fatal(err)
}
```

## 4. Produce a Message

```go
import (
"github.com/weyoss/go-redis-smq/pkg/message/msg"
)

producer := redissmq.NewProducer()
if err := producer.Run(ctx); err != nil {
log.Fatal(err)
}

m := msg.New().SetBody("Hello World").SetQueue(ordersQueue)
ids, err := producer.Produce(ctx, m)
if err != nil {
log.Fatal(err)
}
log.Printf("Sent: %v", ids)
```

## 5. Consume Messages

```go
consumer := redissmq.NewConsumer()
consumer.Consume(ordersQueue, func (ctx context.Context, m *msg.Transferable) error {
log.Printf("Received: %v", m.Body)
return nil // return error to trigger retry
})

if err := consumer.Run(ctx); err != nil {
log.Fatal(err)
}
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
- [Shared Concepts](../../../docs/README.md) — Language-agnostic documentation