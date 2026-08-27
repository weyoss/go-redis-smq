package testutil

import (
	"context"
	"testing"

	"github.com/weyoss/go-redis-smq"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// CreateQueue is a test helper that creates a queue using the internal
// queue manager implementation. This is appropriate for internal test
// utilities and avoids dependency on public convenience functions.
func CreateQueue(tb testing.TB, ctx context.Context, params *publicqueue.Params, queueType publicqueue.Type, deliveryModel publicqueue.DeliveryModel) {
	tb.Helper()
	qm := internalqueue.NewQueueManager()
	if err := qm.Create(ctx, params, queueType, deliveryModel); err != nil {
		tb.Fatalf("create queue %s: %v", params.Name(), err)
	}
}

// StartProducer is a test helper that starts a producer and registers cleanup.
// It returns the public producer interface.
func StartProducer(tb testing.TB, ctx context.Context) producer.Producer {
	tb.Helper()
	prod := redissmq.NewProducer()
	if err := prod.Run(ctx); err != nil {
		tb.Fatalf("run producer: %v", err)
	}
	tb.Cleanup(func() { prod.Shutdown(ctx) })
	return prod
}
