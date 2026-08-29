/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

// Props holds the stored configuration of an exchange.
type Props struct {
	// Type defines the routing strategy (Direct, Fanout, Topic).
	Type Type `json:"type"`

	// Policy restricts which queue types can bind to this exchange.
	Policy Policy `json:"queuePolicy"`
}
