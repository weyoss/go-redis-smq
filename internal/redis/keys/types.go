/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package keys

// QueueKeys holds all Redis key names for a queue
type QueueKeys struct {
	Properties       string // Queue metadata and configuration
	Published        string // Messages waiting to be processed
	Pending          string // Messages being processed (consumer group scope)
	Priority         string // Priority queue messages (consumer group scope)
	DeadLetter       string // Failed messages that couldn't be processed
	Processing       string // Currently processing messages
	Acknowledged     string // Successfully processed messages
	Scheduled        string // Messages scheduled for future delivery
	Delayed          string // Messages waiting for delay to expire
	Requeued         string // Messages returned to queue for retry
	Consumers        string // Active consumer instances
	ConsumerGroups   string // Consumer groups registered
	StateHistory     string // Queue state change history
	ExchangeBindings string // Exchange to queue bindings
	ProcessingQueues string // Internal processing tracking
	WorkersLock      string // Distributed lock for workers
	RateLimit        string // Rate limiting configuration
}

// ConsumerKeys holds keys for an individual consumer
type ConsumerKeys struct {
	Processing string // Messages this consumer is currently handling
}

// ConsumerGroupKeys holds keys for a consumer group
type ConsumerGroupKeys struct {
	Members string // Consumers belonging to this group
}

// NamespaceKeys holds Redis keys for a namespace
type NamespaceKeys struct {
	Queues    string // All queues in this namespace
	Exchanges string // All exchanges in this namespace
}

// ExchangeKeys holds basic exchange information
type ExchangeKeys struct {
	Exchange   string // The exchange itself
	Properties string // Exchange configuration
}

// DirectExchangeKeys holds keys for direct exchanges
type DirectExchangeKeys struct {
	RoutingKeys string // All routing keys for this exchange
}

// DirectRoutingKeyKeys holds keys for a routing key binding
type DirectRoutingKeyKeys struct {
	Queues string // Queues bound to this routing key
}

// TopicExchangeKeys holds keys for topic exchanges
type TopicExchangeKeys struct {
	Patterns string // All binding patterns
}

// TopicBindingKeys holds keys for a topic pattern binding
type TopicBindingKeys struct {
	Queues string // Queues bound to this pattern
}

// FanoutExchangeKeys holds keys for fanout exchanges
type FanoutExchangeKeys struct {
	Queues string // All queues bound to this exchange
}

// SystemKeys holds global system keys
type SystemKeys struct {
	Config           string // System configuration
	AllQueues        string // Registry of all queues
	AllExchanges     string // Registry of all exchanges
	AllNamespaces    string // Registry of all namespaces
	PurgeJobs        string // Queue purging jobs
	PendingPurgeJobs string // Pending purge jobs
	ActivePurgeJobs  string // Currently processing purge jobs
}

// JobKeys holds background job keys
type JobKeys struct {
	Worker string // Worker processing this job
}

// WorkerKeys holds system worker keys
type WorkerKeys struct {
	Heartbeat string // Worker health check
}

// ConsumerSystemKeys holds consumer system keys
type ConsumerSystemKeys struct {
	Queues    string // Queues this consumer is subscribed to
	Heartbeat string // Consumer health check
}

// MessageKeys holds message storage keys
type MessageKeys struct {
	Data                  string // Message content
	AcknowledgmentHistory string // Message processing history
}
