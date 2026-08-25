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

	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalmessage "github.com/weyoss/go-redis-smq/internal/message"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	loggercfg "github.com/weyoss/go-redis-smq/internal/util/logger/cfg"
	"github.com/weyoss/go-redis-smq/pkg/config"
	publicconsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	publiceventbus "github.com/weyoss/go-redis-smq/pkg/eventbus"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
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
	producers []publicproducer.Producer
	consumers []publicconsumer.Consumer
}

var instances trackedInstances

func registerProducer(p publicproducer.Producer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.producers = append(instances.producers, p)
}

func registerConsumer(c publicconsumer.Consumer) {
	instances.mu.Lock()
	defer instances.mu.Unlock()
	instances.consumers = append(instances.consumers, c)
}

// userBusAdapter adapts the internal event bus to the public event bus interface.
type userBusAdapter struct {
	bus *eventbus.EventBus
}

func (a *userBusAdapter) Subscribe(handler func(eventName string, args []interface{}), eventName string) (publiceventbus.Subscription, error) {
	sub, err := a.bus.Subscribe(handler, eventName)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Init initialises RedisSMQ and starts the internal system event bus.
//
// The public user event bus is not started automatically. Applications that
// want to expose events to external subscribers must call
// InitUserEventBus(ctx) separately.
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

	purgeWorkerStop = internalqueue.StartPurgeWorker(systemCtx)

	// Auto-shutdown when the context is cancelled.
	go func() {
		<-ctx.Done()
		Shutdown()
	}()

	initialized = true
	logger.New("redissmq").Info("RedisSMQ initialized successfully")
	return nil
}

// InitUserEventBus starts the public user event bus and makes it available to
// public subscription packages.
func InitUserEventBus(ctx context.Context) {
	bus := eventbus.InitUser(ctx)
	publiceventbus.SetUserBus(&userBusAdapter{bus: bus})
}

// ShutdownUserEventBus clears the public user event bus and shuts down the
// underlying internal user bus.
func ShutdownUserEventBus() {
	publiceventbus.SetUserBus(nil)
	eventbus.ShutdownUser()
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

	// Stop the purge worker first.
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
	consumers := make([]publicconsumer.Consumer, len(instances.consumers))
	copy(consumers, instances.consumers)
	producers := make([]publicproducer.Producer, len(instances.producers))
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

	// Shut down the public user event bus if it was initialised.
	ShutdownUserEventBus()

	// Shut down the internal system bus.
	eventbus.ShutdownSystem()

	l.Info("RedisSMQ shut down complete")

	logger.Shutdown()
	config.Close()
	redis.Close()

	initialized = false
}

// NewProducer creates a new producer that implements the public producer
// interface and registers it for lifecycle management.
func NewProducer() publicproducer.Producer {
	p := internalproducer.New()
	registerProducer(p)
	return p
}

// NewConsumer creates a new consumer that implements the public consumer
// interface and registers it for lifecycle management.
func NewConsumer(opts ...publicconsumer.Option) publicconsumer.Consumer {
	cons := internalconsumer.New(opts...)
	registerConsumer(cons)
	return cons
}

// NewQueueManager creates a new queue manager that implements the public
// queue manager interface.
func NewQueueManager() publicqueue.QueueManager {
	return internalqueue.NewQueueManager()
}

// NewStateManager creates a new state manager that implements the public
// state manager interface.
func NewStateManager() publicqueue.StateManager {
	return internalqueue.NewStateManager()
}

// NewConsumerGroupManager creates a new consumer group manager that implements
// the public consumer group manager interface.
func NewConsumerGroupManager() publicqueue.ConsumerGroupManager {
	return internalqueue.NewConsumerGroupManager()
}

// NewMessageManager creates a new message manager that implements the public
// message manager interface.
func NewMessageManager() publicmessage.MessageManager {
	return internalmessage.NewManager()
}
