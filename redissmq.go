/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package redissmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	loggercfg "github.com/weyoss/go-redis-smq/internal/util/logger/cfg"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/consumer"
	"github.com/weyoss/go-redis-smq/pkg/consumer/c"
	"github.com/weyoss/go-redis-smq/pkg/producer"
)

// Config is the Redis connection configuration.
type Config = redis.Config

var (
	systemCtx       context.Context
	systemStop      context.CancelFunc
	purgeWorkerStop func()

	lifecycleMu sync.Mutex
	initialized bool
)

type trackedInstances struct {
	mu        sync.Mutex
	producers []*producer.Producer
	consumers []*consumer.Consumer
}

var instances trackedInstances

func registerProducer(p *producer.Producer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.producers = append(instances.producers, p)
}

func registerConsumer(c *consumer.Consumer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.consumers = append(instances.consumers, c)
}

// Init initialises RedisSMQ
func Init(ctx context.Context, cfg Config) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if initialized {
		return nil
	}

	if err := redis.Init(ctx, cfg); err != nil {
		return fmt.Errorf("redissmq: redis init failed: %w", err)
	}

	// Always initialise and start the system bus.
	eventbus.InitSystem(ctx)

	if err := config.Init(ctx); err != nil {
		return fmt.Errorf("redissmq: config init failed: %w", err)
	}

	logger.Init(loggercfg.Provider())

	systemCtx, systemStop = context.WithCancel(ctx)

	purgeWorkerStop = internalQueue.StartPurgeWorker(systemCtx)

	// Auto-shutdown when the context is cancelled.
	go func() {
		<-ctx.Done()
		Shutdown()
	}()

	initialized = true
	logger.New("redissmq").Info("RedisSMQ initialized successfully")
	return nil
}

// Shutdown gracefully stops RedisSMQ.
//
// It stops the purge worker, consumers, producers, event buses, logger,
// configuration, and Redis client. Shutdown is safe to call multiple times
// and supports being called again after a new Init.
func Shutdown() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if !initialized {
		return
	}

	l := logger.New("redissmq")
	l.Info("RedisSMQ shutting down...")

	// Stop the purge worker first. This cancels its context and waits
	// briefly for it to exit cleanly.
	if purgeWorkerStop != nil {
		purgeWorkerStop()
		purgeWorkerStop = nil
	}

	// Cancel the system context to signal other background workers.
	if systemStop != nil {
		systemStop()
		systemStop = nil
		systemCtx = nil
	}

	instances.mu.Lock()
	consumers := make([]*consumer.Consumer, len(instances.consumers))
	copy(consumers, instances.consumers)
	producers := make([]*producer.Producer, len(instances.producers))
	copy(producers, instances.producers)
	instances.consumers = nil
	instances.producers = nil
	instances.mu.Unlock()

	for _, c := range consumers {
		c.Shutdown()
	}
	l.Info("consumers shut down", "count", len(consumers))

	bgCtx := context.Background()
	for _, p := range producers {
		p.Shutdown(bgCtx)
	}
	l.Info("producers shut down", "count", len(producers))

	// Shut down the public user bus if it was initialised.
	eventbus.ShutdownUser()

	// Shut down the internal system bus.
	eventbus.ShutdownSystem()

	l.Info("RedisSMQ shut down complete")

	logger.Shutdown()
	config.Close()
	redis.Close()

	initialized = false
}

// NewProducer creates a new producer and registers it for lifecycle
// management.
func NewProducer() *producer.Producer {
	p := producer.New()
	registerProducer(p)
	return p
}

// NewConsumer creates a new consumer and registers it for lifecycle
// management.
func NewConsumer(opts ...c.Option) *consumer.Consumer {
	cons := consumer.New(opts...)
	registerConsumer(cons)
	return cons
}
