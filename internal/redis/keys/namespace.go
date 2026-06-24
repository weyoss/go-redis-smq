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

// Namespace represents a logical isolation boundary
type Namespace struct {
	Name string
}

// Queues returns the key for all queues in this namespace
func (ns Namespace) Queues() string {
	return Key("ns", ns.Name, "q")
}

// Exchanges returns the key for all exchanges in this namespace
func (ns Namespace) Exchanges() string {
	return Key("ns", ns.Name, "exs")
}
