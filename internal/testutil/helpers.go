/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package testutil

import (
	"context"
	"testing"

	"github.com/weyoss/go-redis-smq"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	"github.com/weyoss/go-redis-smq/pkg/queue"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

func CreateQueue(tb testing.TB, ctx context.Context, params *q.QueueParams, queueType q.QueueType, deliveryModel q.DeliveryModel) {
	tb.Helper()
	if err := queue.Create(ctx, params, queueType, deliveryModel); err != nil {
		tb.Fatalf("create queue %s: %v", params.Name(), err)
	}
}

func StartProducer(tb testing.TB, ctx context.Context) publicproducer.Producer {
	tb.Helper()
	prod := redissmq.NewProducer()
	if err := prod.Run(ctx); err != nil {
		tb.Fatalf("run producer: %v", err)
	}
	tb.Cleanup(func() { prod.Shutdown(ctx) })
	return prod
}
