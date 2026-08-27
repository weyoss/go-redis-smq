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
	"context"

	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
)

// Manager is the concrete implementation of the public message manager.
type Manager struct {
	store *Store
}

// NewManager creates a new internal message manager that satisfies the
// public message.Manager interface.
func NewManager() publicmessage.Manager {
	return &Manager{
		store: NewStore(NewEnvelopeCodec(), NewStateCodec()),
	}
}

func (m *Manager) Status(ctx context.Context, messageID string) (publicmessage.MessageStatus, error) {
	return m.store.GetStatus(ctx, messageID)
}

func (m *Manager) State(ctx context.Context, messageID string) (*publicmessage.StateTransferable, error) {
	state, err := m.store.GetState(ctx, messageID)
	if err != nil {
		return nil, err
	}
	s := state.ToTransferable()
	return &s, nil
}

func (m *Manager) Get(ctx context.Context, messageID string) (*publicmessage.Transferable, error) {
	env, err := m.store.GetMessage(ctx, messageID)
	if err != nil {
		return nil, err
	}
	return env.ToTransferable(), nil
}

func (m *Manager) GetAll(ctx context.Context, messageIDs []string) ([]publicmessage.Transferable, error) {
	envelopes, err := m.store.GetMessages(ctx, messageIDs)
	if err != nil {
		return nil, err
	}
	out := make([]publicmessage.Transferable, len(envelopes))
	for i, env := range envelopes {
		out[i] = *env.ToTransferable()
	}
	return out, nil
}

func (m *Manager) Delete(ctx context.Context, messageID string) (*publicmessage.DeleteResponse, error) {
	return m.DeleteAll(ctx, []string{messageID})
}

func (m *Manager) DeleteAll(ctx context.Context, messageIDs []string) (*publicmessage.DeleteResponse, error) {
	if len(messageIDs) == 0 {
		return &publicmessage.DeleteResponse{Status: publicmessage.DeleteStatusOK}, nil
	}
	return m.store.DeleteMessages(ctx, messageIDs, "")
}

func (m *Manager) Requeue(ctx context.Context, messageID string) (string, error) {
	return m.store.RequeueMessage(ctx, messageID)
}

func (m *Manager) UnacknowledgmentHistory(ctx context.Context, messageID string) ([]string, error) {
	return m.store.GetUnacknowledgmentHistory(ctx, messageID)
}
