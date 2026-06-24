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

// Message returns the key for message content
func (System) Message(messageID string) string {
	return Key("main", "msg", messageID)
}

// MessageAcknowledgementHistory returns the key for message processing history
func (System) MessageAcknowledgementHistory(messageID string) string {
	return Key("main", "msg", messageID, "uh")
}
