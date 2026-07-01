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

// Manager provides Redis-backed message operations.
// Wires together the store with default codecs.
type Manager struct {
	store *Store
}

// NewManager creates a new message manager with default codecs.
func NewManager() *Manager {
	return &Manager{
		store: NewStore(NewEnvelopeCodec(), NewStateCodec()),
	}
}

// Store returns the message store for persistence operations.
func (m *Manager) Store() *Store { return m.store }
