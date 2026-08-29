/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message

import (
	"strings"
	"time"

	"github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// ProducibleMessage configures a message for production to queues or exchanges.
//
// It uses a builder pattern for fluent configuration.
//
// Example:
//
//	m := message.New().
//	    SetBody(map[string]interface{}{"userId": 123}).
//	    SetQueue(queueParams).
//	    SetTTL(5 * time.Minute).
//	    SetPriority(message.PriorityHigh)
type ProducibleMessage struct {
	createdAt             time.Time
	ttl                   time.Duration
	retryThreshold        int
	retryDelay            time.Duration
	consumeTimeout        time.Duration
	body                  interface{}
	priority              *Priority
	scheduledCron         string
	scheduledDelay        *time.Duration
	scheduledRepeatPeriod *time.Duration
	scheduledRepeat       int
	exchange              *exchange.Params
	exchangeRoutingKey    string
	queue                 *queue.Params
}

// defaultOptions holds the default consume options for all instances.
var defaultOptions = DefaultConsumeOptions()

// New creates a new ProducibleMessage with default consume options.
func New() *ProducibleMessage {
	now := time.Now()
	return &ProducibleMessage{
		createdAt:      now,
		ttl:            time.Duration(defaultOptions.TTL) * time.Millisecond,
		retryThreshold: defaultOptions.RetryThreshold,
		retryDelay:     time.Duration(defaultOptions.RetryDelay) * time.Millisecond,
		consumeTimeout: time.Duration(defaultOptions.ConsumeTimeout) * time.Millisecond,
	}
}

// SetDefaultConsumeOptions sets the default options for all future messages.
func SetDefaultConsumeOptions(opts ConsumeOptions) {
	if opts.TTL >= 0 {
		defaultOptions.TTL = opts.TTL
	}
	if opts.RetryThreshold >= 0 {
		defaultOptions.RetryThreshold = opts.RetryThreshold
	}
	if opts.RetryDelay >= 0 {
		defaultOptions.RetryDelay = opts.RetryDelay
	}
	if opts.ConsumeTimeout >= 0 {
		defaultOptions.ConsumeTimeout = opts.ConsumeTimeout
	}
}

// CreatedAt returns the message creation timestamp.
func (m *ProducibleMessage) CreatedAt() time.Time { return m.createdAt }

// TTL returns the time-to-live duration.
func (m *ProducibleMessage) TTL() time.Duration { return m.ttl }

// SetTTL sets the time-to-live for the message. Negative values are treated
// as zero (no expiration).
func (m *ProducibleMessage) SetTTL(ttl time.Duration) *ProducibleMessage {
	if ttl < 0 {
		ttl = 0
	}
	m.ttl = ttl
	return m
}

// RetryThreshold returns the maximum retry attempts.
func (m *ProducibleMessage) RetryThreshold() int { return m.retryThreshold }

// SetRetryThreshold sets the maximum number of retry attempts.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetRetryThreshold(threshold int) *ProducibleMessage {
	if threshold < 0 {
		threshold = 0
	}
	m.retryThreshold = threshold
	return m
}

// RetryDelay returns the delay between retry attempts.
func (m *ProducibleMessage) RetryDelay() time.Duration { return m.retryDelay }

// SetRetryDelay sets the delay between retry attempts.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetRetryDelay(delay time.Duration) *ProducibleMessage {
	if delay < 0 {
		delay = 0
	}
	m.retryDelay = delay
	return m
}

// ConsumeTimeout returns the consumption timeout.
func (m *ProducibleMessage) ConsumeTimeout() time.Duration { return m.consumeTimeout }

// SetConsumeTimeout sets the maximum time for message consumption.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetConsumeTimeout(timeout time.Duration) *ProducibleMessage {
	if timeout < 0 {
		timeout = 0
	}
	m.consumeTimeout = timeout
	return m
}

// Body returns the message payload.
func (m *ProducibleMessage) Body() interface{} { return m.body }

// SetBody sets the message payload. The payload must be JSON-serializable.
func (m *ProducibleMessage) SetBody(body interface{}) *ProducibleMessage {
	m.body = body
	return m
}

// Priority returns the message priority, or nil if not set.
func (m *ProducibleMessage) Priority() *Priority { return m.priority }

// SetPriority sets the priority level for the message.
func (m *ProducibleMessage) SetPriority(priority Priority) *ProducibleMessage {
	m.priority = &priority
	return m
}

// HasPriority reports whether a priority has been set.
func (m *ProducibleMessage) HasPriority() bool { return m.priority != nil }

// DisablePriority removes the priority setting.
func (m *ProducibleMessage) DisablePriority() *ProducibleMessage {
	m.priority = nil
	return m
}

// ScheduledCron returns the CRON expression.
func (m *ProducibleMessage) ScheduledCron() string { return m.scheduledCron }

// SetScheduledCron sets a CRON expression for scheduled delivery.
// No validation is performed here; validation occurs in the internal
// scheduler if needed.
func (m *ProducibleMessage) SetScheduledCron(cronExpr string) *ProducibleMessage {
	m.scheduledCron = strings.TrimSpace(cronExpr)
	return m
}

// ScheduledDelay returns the scheduled delay, or nil if not set.
func (m *ProducibleMessage) ScheduledDelay() *time.Duration { return m.scheduledDelay }

// SetScheduledDelay sets a delay before initial delivery.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetScheduledDelay(delay time.Duration) *ProducibleMessage {
	if delay < 0 {
		delay = 0
	}
	m.scheduledDelay = &delay
	return m
}

// ScheduledRepeatPeriod returns the repeat period, or nil if not set.
func (m *ProducibleMessage) ScheduledRepeatPeriod() *time.Duration {
	return m.scheduledRepeatPeriod
}

// SetScheduledRepeatPeriod sets the repeat period for scheduled delivery.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetScheduledRepeatPeriod(period time.Duration) *ProducibleMessage {
	if period < 0 {
		period = 0
	}
	m.scheduledRepeatPeriod = &period
	return m
}

// ScheduledRepeat returns the repeat count.
func (m *ProducibleMessage) ScheduledRepeat() int { return m.scheduledRepeat }

// SetScheduledRepeat sets the number of times to repeat after initial delivery.
// Negative values are treated as zero.
func (m *ProducibleMessage) SetScheduledRepeat(repeat int) *ProducibleMessage {
	if repeat < 0 {
		repeat = 0
	}
	m.scheduledRepeat = repeat
	return m
}

// ResetScheduledParams resets all scheduling parameters.
func (m *ProducibleMessage) ResetScheduledParams() *ProducibleMessage {
	m.scheduledCron = ""
	m.scheduledDelay = nil
	m.scheduledRepeatPeriod = nil
	m.scheduledRepeat = 0
	return m
}

// Exchange returns the exchange configuration, if any.
func (m *ProducibleMessage) Exchange() *exchange.Params { return m.exchange }

// ExchangeRoutingKey returns the exchange routing key.
func (m *ProducibleMessage) ExchangeRoutingKey() string { return m.exchangeRoutingKey }

// SetDirectExchange sets a direct exchange for routing.
// It clears any previously set queue.
func (m *ProducibleMessage) SetDirectExchange(params *exchange.Params) *ProducibleMessage {
	m.exchange = params
	m.queue = nil
	m.exchangeRoutingKey = ""
	return m
}

// SetFanoutExchange sets a fanout exchange for broadcasting.
// It clears any previously set queue.
func (m *ProducibleMessage) SetFanoutExchange(params *exchange.Params) *ProducibleMessage {
	m.exchange = params
	m.queue = nil
	m.exchangeRoutingKey = ""
	return m
}

// SetTopicExchange sets a topic exchange for pattern routing.
// It clears any previously set queue.
func (m *ProducibleMessage) SetTopicExchange(params *exchange.Params) *ProducibleMessage {
	m.exchange = params
	m.queue = nil
	m.exchangeRoutingKey = ""
	return m
}

// SetExchangeRoutingKey sets the routing key for exchange-based delivery.
func (m *ProducibleMessage) SetExchangeRoutingKey(key string) *ProducibleMessage {
	m.exchangeRoutingKey = key
	return m
}

// Queue returns the target queue, if any.
func (m *ProducibleMessage) Queue() *queue.Params { return m.queue }

// SetQueue sets the target queue for direct delivery.
// It clears any previously set exchange.
func (m *ProducibleMessage) SetQueue(params *queue.Params) *ProducibleMessage {
	m.queue = params
	m.exchange = nil
	m.exchangeRoutingKey = ""
	return m
}

// ToParams converts the message to a serializable params struct.
func (m *ProducibleMessage) ToParams(destinationQueue *queue.Params, consumerGroupID string) *Params {
	p := &Params{
		CreatedAt:        m.createdAt.UnixMilli(),
		TTL:              m.ttl.Milliseconds(),
		RetryThreshold:   m.retryThreshold,
		RetryDelay:       m.retryDelay.Milliseconds(),
		ConsumeTimeout:   m.consumeTimeout.Milliseconds(),
		Body:             m.body,
		Priority:         m.priority,
		ScheduledCron:    m.scheduledCron,
		ScheduledRepeat:  m.scheduledRepeat,
		Exchange:         m.exchange,
		Queue:            m.queue,
		DestinationQueue: destinationQueue,
		ConsumerGroupID:  consumerGroupID,
	}

	if m.scheduledDelay != nil {
		v := m.scheduledDelay.Milliseconds()
		p.ScheduledDelay = &v
	}
	if m.scheduledRepeatPeriod != nil {
		v := m.scheduledRepeatPeriod.Milliseconds()
		p.ScheduledRepeatPeriod = &v
	}

	return p
}
