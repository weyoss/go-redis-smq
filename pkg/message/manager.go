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

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
)

// Manager provides message lifecycle operations.
//
// It allows callers to retrieve messages by ID, delete one or many messages,
// requeue dead-lettered or acknowledged messages, inspect status and state,
// and read unacknowledgment history when audit is enabled.
type Manager struct {
	store *internalMessage.Store
}

// NewManager creates a new message manager with default codecs.
func NewManager() *Manager {
	internalMgr := internalMessage.NewManager()
	return &Manager{
		store: internalMgr.Store(),
	}
}

// Status retrieves the current status of a message.
//
// Example:
//
//	status, err := message.Status(ctx, "msg-123")
func (m *Manager) Status(ctx context.Context, messageID string) (msg.MessageStatus, error) {
	return m.store.GetStatus(ctx, messageID)
}

// State retrieves the lifecycle state of a message.
//
// Example:
//
//	state, err := message.State(ctx, "msg-123")
//	fmt.Println(state.Attempts)
func (m *Manager) State(ctx context.Context, messageID string) (*msg.StateTransferable, error) {
	msgState, err := m.store.GetState(ctx, messageID)
	if err != nil {
		return nil, err
	}
	s := msgState.ToTransferable()
	return &s, nil
}

// Get retrieves a single message by its ID.
//
// Example:
//
//	msg, err := message.Get(ctx, "msg-123")
//	fmt.Println(msg.Body)
func (m *Manager) Get(ctx context.Context, messageID string) (*msg.Transferable, error) {
	envelope, err := m.store.GetMessage(ctx, messageID)
	if err != nil {
		return nil, err
	}
	return envelope.ToTransferable(), nil
}

// GetAll retrieves multiple messages by their IDs.
//
// Messages that are not found are silently skipped.
//
// Example:
//
//	messages, err := message.GetAll(ctx, []string{"msg-1", "msg-2"})
func (m *Manager) GetAll(ctx context.Context, messageIDs []string) ([]msg.Transferable, error) {
	envelopes, err := m.store.GetMessages(ctx, messageIDs)
	if err != nil {
		return nil, err
	}

	result := make([]msg.Transferable, len(envelopes))
	for i, env := range envelopes {
		result[i] = *env.ToTransferable()
	}
	return result, nil
}

// Delete removes a single message by its ID.
//
// Example:
//
//	result, err := message.Delete(ctx, "msg-123")
//	fmt.Println(result.Status)
func (m *Manager) Delete(ctx context.Context, messageID string) (*msg.DeleteResponse, error) {
	return m.DeleteAll(ctx, []string{messageID})
}

// DeleteAll removes multiple messages by their IDs.
//
// Messages are grouped by queue and consumer group for efficient deletion.
//
// Example:
//
//	result, err := message.DeleteAll(ctx, []string{"msg-1", "msg-2"})
//	fmt.Printf("Deleted: %d/%d\n", result.Stats.Success, result.Stats.Processed)
func (m *Manager) DeleteAll(ctx context.Context, messageIDs []string) (*msg.DeleteResponse, error) {
	if len(messageIDs) == 0 {
		return &msg.DeleteResponse{Status: msg.DeleteStatusOK}, nil
	}
	return m.store.DeleteMessages(ctx, messageIDs, "")
}

// Requeue creates a new copy of a message for reprocessing.
//
// The original message must be in Acknowledged or DeadLettered status.
// Returns the new message ID.
//
// Example:
//
//	newID, err := message.Requeue(ctx, "msg-123")
func (m *Manager) Requeue(ctx context.Context, messageID string) (string, error) {
	return m.store.RequeueMessage(ctx, messageID)
}

// UnacknowledgmentHistory retrieves the unacknowledgment history for a message.
//
// Requires message audit to be enabled in configuration.
//
// Example:
//
//	history, err := message.UnacknowledgmentHistory(ctx, "msg-123")
func (m *Manager) UnacknowledgmentHistory(ctx context.Context, messageID string) ([]string, error) {
	return m.store.GetUnacknowledgmentHistory(ctx, messageID)
}

// Default manager instance used by package-level convenience functions.
var defaultManager = NewManager()

// Status retrieves message status using the default manager.
func Status(ctx context.Context, messageID string) (msg.MessageStatus, error) {
	return defaultManager.Status(ctx, messageID)
}

// State retrieves message state using the default manager.
func State(ctx context.Context, messageID string) (*msg.StateTransferable, error) {
	return defaultManager.State(ctx, messageID)
}

// Get retrieves a message using the default manager.
func Get(ctx context.Context, messageID string) (*msg.Transferable, error) {
	return defaultManager.Get(ctx, messageID)
}

// GetAll retrieves messages using the default manager.
func GetAll(ctx context.Context, messageIDs []string) ([]msg.Transferable, error) {
	return defaultManager.GetAll(ctx, messageIDs)
}

// Delete removes a message using the default manager.
func Delete(ctx context.Context, messageID string) (*msg.DeleteResponse, error) {
	return defaultManager.Delete(ctx, messageID)
}

// DeleteAll removes messages using the default manager.
func DeleteAll(ctx context.Context, messageIDs []string) (*msg.DeleteResponse, error) {
	return defaultManager.DeleteAll(ctx, messageIDs)
}

// Requeue creates a message copy using the default manager.
func Requeue(ctx context.Context, messageID string) (string, error) {
	return defaultManager.Requeue(ctx, messageID)
}

// UnacknowledgmentHistory retrieves the unacknowledgment history for a message
// using the default manager.
//
// Requires message audit to be enabled in configuration.
func UnacknowledgmentHistory(ctx context.Context, messageID string) ([]string, error) {
	return defaultManager.UnacknowledgmentHistory(ctx, messageID)
}
