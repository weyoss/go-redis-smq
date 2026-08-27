/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package redissmq is the entry point for the RedisSMQ message queue.
//
// It provides bootstrap and lifecycle management (Init/Shutdown) for the
// RedisSMQ runtime, as well as factory functions that return the concrete
// implementations behind the public interfaces defined in the pkg packages.
//
// The package is designed to be imported only by the application's main
// package or composition root. All other packages should depend on the
// public interfaces rather than this package.
package redissmq

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalexchange "github.com/weyoss/go-redis-smq/internal/exchange"
	internalmessage "github.com/weyoss/go-redis-smq/internal/message"
	internalnamespace "github.com/weyoss/go-redis-smq/internal/namespace"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	internalredis "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/config"
	publicconsumer "github.com/weyoss/go-redis-smq/pkg/consumer"
	publiceventbus "github.com/weyoss/go-redis-smq/pkg/eventbus"
	publicexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
	publicnamespace "github.com/weyoss/go-redis-smq/pkg/namespace"
	publicproducer "github.com/weyoss/go-redis-smq/pkg/producer"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

var (
	// systemCtx holds the root context for the RedisSMQ runtime.
	systemCtx context.Context
	// systemStop cancels the root context.
	systemStop context.CancelFunc
	// purgeWorkerStop stops the background purge worker.
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

// Subscribe implements publiceventbus.EventBus.
func (a *userBusAdapter) Subscribe(handler func(eventName string, args []interface{}), eventName string) (publiceventbus.Subscription, error) {
	sub, err := a.bus.Subscribe(handler, eventName)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Init initializes the RedisSMQ runtime.
//
// It connects to Redis, starts the internal system event bus, loads the
// configuration, and launches background workers (such as the queue purge
// worker). The provided context is used as the parent for all internal
// operations; cancelling it triggers a graceful shutdown.
//
// Init is idempotent and can be called multiple times; subsequent calls
// are no-ops.
func Init(ctx context.Context, cfg redis.Options) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if initialized {
		return nil
	}

	if err := internalredis.Init(ctx, cfg); err != nil {
		return fmt.Errorf("redissmq: redis init failed: %w", err)
	}

	eventbus.InitSystem(ctx)

	if err := internalconfig.Init(ctx); err != nil {
		return fmt.Errorf("redissmq: config init failed: %w", err)
	}

	systemCtx, systemStop = context.WithCancel(ctx)

	purgeWorkerStop = internalqueue.StartPurgeWorker(systemCtx)

	go func() {
		<-ctx.Done()
		Shutdown()
	}()

	initialized = true
	logger.New("redissmq").Info("RedisSMQ initialized successfully")
	return nil
}

// InitUserEventBus starts the public user event bus.
//
// The user event bus is separate from the internal system bus and is used
// to deliver events to external subscribers via the public subscription
// functions in the pkg packages. It is not started automatically; call this
// function if you need to use the public event subscription API.
func InitUserEventBus(ctx context.Context) {
	bus := eventbus.InitUser(ctx)
	publiceventbus.SetUserBus(&userBusAdapter{bus: bus})
}

// ShutdownUserEventBus stops the public user event bus and clears the global
// reference. It can be called independently of the main Shutdown function.
func ShutdownUserEventBus() {
	publiceventbus.SetUserBus(nil)
	eventbus.ShutdownUser()
}

// Shutdown gracefully stops the RedisSMQ runtime.
//
// It stops the purge worker, shuts down all registered consumers and
// producers, stops the event buses, releases the configuration, and closes
// the Redis client. Shutdown is safe to call multiple times and may be
// followed by another Init to restart the runtime.
func Shutdown() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if !initialized {
		return
	}

	l := logger.New("redissmq")
	l.Info("RedisSMQ shutting down...")

	if purgeWorkerStop != nil {
		purgeWorkerStop()
		purgeWorkerStop = nil
	}

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

	ShutdownUserEventBus()
	eventbus.ShutdownSystem()

	l.Info("RedisSMQ shut down complete")

	logger.Shutdown()
	internalconfig.Close()
	internalredis.Close()

	initialized = false
}

// NewProducer returns a new producer that implements publicproducer.Producer.
// The producer is automatically registered for lifecycle management and will
// be shut down when Shutdown is called.
func NewProducer() publicproducer.Producer {
	p := internalproducer.New()
	registerProducer(p)
	return p
}

// NewConsumer returns a new consumer that implements publicconsumer.Consumer.
// The consumer is automatically registered for lifecycle management and will
// be shut down when Shutdown is called.
func NewConsumer(opts ...publicconsumer.Option) publicconsumer.Consumer {
	c := internalconsumer.New(opts...)
	registerConsumer(c)
	return c
}

// NewQueueManager returns a new queue manager that implements
// publicqueue.Manager.
func NewQueueManager() publicqueue.Manager {
	return internalqueue.NewQueueManager()
}

// NewStateManager returns a new state manager that implements
// publicqueue.StateManager.
func NewStateManager() publicqueue.StateManager {
	return internalqueue.NewStateManager()
}

// NewConsumerGroupManager returns a new consumer group manager that
// implements publicqueue.ConsumerGroupManager.
func NewConsumerGroupManager() publicqueue.ConsumerGroupManager {
	return internalqueue.NewConsumerGroupManager()
}

// NewMessageManager returns a new message manager that implements
// publicmessage.Manager.
func NewMessageManager() publicmessage.Manager {
	return internalmessage.NewManager()
}

// NewExchangeManager returns a new exchange manager that implements
// publicexchange.Manager.
func NewExchangeManager() publicexchange.Manager {
	return internalexchange.NewManager()
}

// NewDirectExchange returns a new direct exchange that implements
// publicexchange.DirectExchange.
func NewDirectExchange() publicexchange.DirectExchange {
	return internalexchange.NewManager().Direct()
}

// NewFanoutExchange returns a new fanout exchange that implements
// publicexchange.FanoutExchange.
func NewFanoutExchange() publicexchange.FanoutExchange {
	return internalexchange.NewManager().Fanout()
}

// NewTopicExchange returns a new topic exchange that implements
// publicexchange.TopicExchange.
func NewTopicExchange() publicexchange.TopicExchange {
	return internalexchange.NewManager().Topic()
}

// NewConfigManager returns the singleton configuration manager that implements
// config.Manager.
func NewConfigManager() config.Manager {
	return internalconfig.DefaultManager()
}

// NewNamespaceManager returns a new namespace manager that implements
// publicnamespace.Manager.
func NewNamespaceManager() publicnamespace.Manager {
	return internalnamespace.NewManager()
}
