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
)

// DeleteStatus represents the result of a delete operation.
type DeleteStatus string

const (
	// DeleteStatusOK indicates that all requested messages were successfully
	// deleted.
	DeleteStatusOK DeleteStatus = "OK"

	// DeleteStatusPartialSuccess indicates that some messages were deleted
	// while others could not be deleted.
	DeleteStatusPartialSuccess DeleteStatus = "PARTIAL_SUCCESS"

	// DeleteStatusNotFound indicates that none of the requested messages
	// were found.
	DeleteStatusNotFound DeleteStatus = "MESSAGE_NOT_FOUND"

	// DeleteStatusInProcess indicates that the requested messages are
	// currently being processed and cannot be deleted.
	DeleteStatusInProcess DeleteStatus = "MESSAGE_IN_PROCESS"

	// DeleteStatusNotDeleted indicates that the requested messages could not
	// be deleted.
	DeleteStatusNotDeleted DeleteStatus = "MESSAGE_NOT_DELETED"
)

// DeleteStats holds counters for a delete operation.
type DeleteStats struct {
	// Processed is the total number of message IDs examined.
	Processed int `json:"processed"`

	// Success is the number of messages successfully deleted.
	Success int `json:"success"`

	// NotFound is the number of message IDs that did not correspond to an
	// existing message.
	NotFound int `json:"notFound"`

	// InProcess is the number of messages that were skipped because they are
	// currently being processed.
	InProcess int `json:"inProcess"`
}

// DeleteResponse contains the result of a message deletion operation.
type DeleteResponse struct {
	// Status summarizes the overall result of the operation.
	Status DeleteStatus `json:"status"`

	// Stats contains detailed counters for the operation.
	Stats DeleteStats `json:"stats"`
}

// Manager is the public interface for message lifecycle operations.
type Manager interface {
	// Status retrieves the current status of a message.
	Status(ctx context.Context, messageID string) (Status, error)

	// State retrieves the lifecycle state of a message.
	State(ctx context.Context, messageID string) (*StateTransferable, error)

	// Get retrieves a single message by its ID.
	Get(ctx context.Context, messageID string) (*Transferable, error)

	// GetAll retrieves multiple messages by their IDs.
	// Missing messages are silently skipped.
	GetAll(ctx context.Context, messageIDs []string) ([]Transferable, error)

	// Delete removes a single message by its ID.
	Delete(ctx context.Context, messageID string) (*DeleteResponse, error)

	// DeleteAll removes multiple messages by their IDs.
	DeleteAll(ctx context.Context, messageIDs []string) (*DeleteResponse, error)

	// Requeue creates a new copy of a message for reprocessing.
	// Returns the new message ID.
	Requeue(ctx context.Context, messageID string) (string, error)

	// UnacknowledgmentHistory retrieves the unacknowledgment history for a message.
	// Requires message audit to be enabled.
	UnacknowledgmentHistory(ctx context.Context, messageID string) ([]string, error)
}
