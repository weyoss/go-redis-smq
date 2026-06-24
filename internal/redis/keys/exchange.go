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

// Exchange represents a message router
type Exchange struct {
	Namespace string
	Name      string
}

// Exchange returns the key for the exchange itself
func (e Exchange) Exchange() string {
	return Key("ns", e.Namespace, "exs", e.Name)
}

// Properties returns the key for exchange configuration
func (e Exchange) Properties() string {
	return Key("ns", e.Namespace, "exs", e.Name, "prop")
}

// RoutingKeys returns the key for all routing keys in direct exchange
func (e Exchange) RoutingKeys() string {
	return Key("ns", e.Namespace, "exs", e.Name, "rk")
}

// RoutingKeyQueues returns the key for queues bound to a specific routing key
func (e Exchange) RoutingKeyQueues(routingKey string) string {
	return Key("ns", e.Namespace, "exs", e.Name, "rk", routingKey, "q")
}

// BindingPatterns returns the key for all binding patterns in topic exchange
func (e Exchange) BindingPatterns() string {
	return Key("ns", e.Namespace, "exs", e.Name, "pat")
}

// PatternQueues returns the key for queues bound to a specific topic pattern
func (e Exchange) PatternQueues(pattern string) string {
	return Key("ns", e.Namespace, "exs", e.Name, "pat", pattern, "q")
}

// FanoutQueues returns the key for all queues bound to fanout exchange
func (e Exchange) FanoutQueues() string {
	return Key("ns", e.Namespace, "exs", e.Name, "q")
}
