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

	internalconfig "github.com/weyoss/go-redis-smq/internal/config"
	internalconsumer "github.com/weyoss/go-redis-smq/internal/consumer"
	"github.com/weyoss/go-redis-smq/internal/eventbus"
	internalexchange "github.com/weyoss/go-redis-smq/internal/exchange"
	internalmessage "github.com/weyoss/go-redis-smq/internal/message"
	internalnamespace "github.com/weyoss/go-redis-smq/internal/namespace"
	internalproducer "github.com/weyoss/go-redis-smq/internal/producer"
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	"github.com/weyoss/go-redis-smq/internal/redis"
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
func Init(ctx context.Context, cfg Config) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()

	if initialized {
		return nil
	}

	if err := redis.Init(ctx, cfg); err != nil {
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
func InitUserEventBus(ctx context.Context) {
	bus := eventbus.InitUser(ctx)
	publiceventbus.SetUserBus(&userBusAdapter{bus: bus})
}

// ShutdownUserEventBus clears the public user event bus.
func ShutdownUserEventBus() {
	publiceventbus.SetUserBus(nil)
	eventbus.ShutdownUser()
}

// Shutdown gracefully stops RedisSMQ.
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
	redis.Close()

	initialized = false
}

// NewProducer creates a new producer.
func NewProducer() publicproducer.Producer {
	p := internalproducer.New()
	registerProducer(p)
	return p
}

// NewConsumer creates a new consumer.
func NewConsumer(opts ...publicconsumer.Option) publicconsumer.Consumer {
	c := internalconsumer.New(opts...)
	registerConsumer(c)
	return c
}

// NewQueueManager creates a new queue manager.
func NewQueueManager() publicqueue.QueueManager {
	return internalqueue.NewQueueManager()
}

// NewStateManager creates a new state manager.
func NewStateManager() publicqueue.StateManager {
	return internalqueue.NewStateManager()
}

// NewConsumerGroupManager creates a new consumer group manager.
func NewConsumerGroupManager() publicqueue.ConsumerGroupManager {
	return internalqueue.NewConsumerGroupManager()
}

// NewMessageManager creates a new message manager.
func NewMessageManager() publicmessage.MessageManager {
	return internalmessage.NewManager()
}

// NewExchangeManager creates a new exchange manager.
func NewExchangeManager() publicexchange.Manager {
	return internalexchange.NewManager()
}

// NewDirectExchange creates a new direct exchange.
func NewDirectExchange() publicexchange.DirectExchange {
	return internalexchange.NewManager().Direct()
}

// NewFanoutExchange creates a new fanout exchange.
func NewFanoutExchange() publicexchange.FanoutExchange {
	return internalexchange.NewManager().Fanout()
}

// NewTopicExchange creates a new topic exchange.
func NewTopicExchange() publicexchange.TopicExchange {
	return internalexchange.NewManager().Topic()
}

// NewConfigManager returns the singleton configuration manager.
func NewConfigManager() config.Manager {
	return internalconfig.DefaultManager()
}

// NewNamespaceManager creates a new namespace manager.
func NewNamespaceManager() publicnamespace.Manager {
	return internalnamespace.NewManager()
}
