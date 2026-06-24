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

// Queue represents a message queue in a namespace
type Queue struct {
	Namespace string
	Name      string
}

// Properties returns the key for queue metadata and configuration
func (q Queue) Properties() string {
	return Key("ns", q.Namespace, "q", q.Name, "prop")
}

// Published returns the key for published messages (all messages in the queue)
func (q Queue) Published() string {
	return Key("ns", q.Namespace, "q", q.Name, "pub")
}

// Pending returns the key for messages waiting to be processed
func (q Queue) Pending() string {
	return Key("ns", q.Namespace, "q", q.Name, "pend")
}

// Priority returns the key for priority queue messages waiting to be processed
func (q Queue) Priority() string {
	return Key("ns", q.Namespace, "q", q.Name, "prio")
}

// DeadLetter returns the key for failed messages that couldn't be processed
func (q Queue) DeadLetter() string {
	return Key("ns", q.Namespace, "q", q.Name, "dl")
}

// Acknowledged returns the key for successfully processed messages
func (q Queue) Acknowledged() string {
	return Key("ns", q.Namespace, "q", q.Name, "ack")
}

// Scheduled returns the key for messages scheduled for future delivery
func (q Queue) Scheduled() string {
	return Key("ns", q.Namespace, "q", q.Name, "sched")
}

// Delayed returns the key for messages waiting for delay to expire
func (q Queue) Delayed() string {
	return Key("ns", q.Namespace, "q", q.Name, "dly")
}

// Requeued returns the key for messages returned to queue for retry
func (q Queue) Requeued() string {
	return Key("ns", q.Namespace, "q", q.Name, "req")
}

// Consumers returns the key for active consumer instances
func (q Queue) Consumers() string {
	return Key("ns", q.Namespace, "q", q.Name, "cons")
}

// ConsumerGroups returns the key for consumer groups registered
func (q Queue) ConsumerGroups() string {
	return Key("ns", q.Namespace, "q", q.Name, "cgp")
}

// StateHistory returns the key for queue state change history
func (q Queue) StateHistory() string {
	return Key("ns", q.Namespace, "q", q.Name, "sh")
}

// ExchangeBindings returns the key for exchange to queue bindings
func (q Queue) ExchangeBindings() string {
	return Key("ns", q.Namespace, "q", q.Name, "bind")
}

// ProcessingQueues returns the key for internal processing tracking
func (q Queue) ProcessingQueues() string {
	return Key("ns", q.Namespace, "q", q.Name, "proc-q")
}

// WorkersLock returns the key for distributed lock for workers
func (q Queue) WorkersLock() string {
	return Key("ns", q.Namespace, "q", q.Name, "wlock")
}

// RateLimit returns the key for rate limiting configuration
func (q Queue) RateLimit() string {
	return Key("ns", q.Namespace, "q", q.Name, "rate")
}

// PendingWithGroup returns the key for pending messages in a specific consumer group
func (q Queue) PendingWithGroup(groupID string) string {
	return Key("ns", q.Namespace, "q", q.Name, "cgp", groupID, "pend")
}

// PriorityWithGroup returns the key for priority messages in a specific consumer group waiting to be processed
func (q Queue) PriorityWithGroup(groupID string) string {
	return Key("ns", q.Namespace, "q", q.Name, "cgp", groupID, "prio")
}

// ConsumerProcessing returns the key for messages a specific consumer is handling
func (q Queue) ConsumerProcessing(consumerID string) string {
	return Key("ns", q.Namespace, "q", q.Name, "cons", consumerID, "proc")
}

// ConsumerGroupMembers returns the key for consumers belonging to a group
func (q Queue) ConsumerGroupMembers(groupID string) string {
	return Key("ns", q.Namespace, "q", q.Name, "cgp", groupID, "cons")
}
