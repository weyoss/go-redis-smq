/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package msg

// DeleteStatus represents the result of a delete operation.
type DeleteStatus string

const (
	// DeleteStatusOK indicates all messages were successfully deleted.
	DeleteStatusOK DeleteStatus = "OK"

	// DeleteStatusPartialSuccess indicates some messages were deleted.
	DeleteStatusPartialSuccess DeleteStatus = "PARTIAL_SUCCESS"

	// DeleteStatusNotFound indicates no messages were found.
	DeleteStatusNotFound DeleteStatus = "MESSAGE_NOT_FOUND"

	// DeleteStatusInProcess indicates messages are currently being processed.
	DeleteStatusInProcess DeleteStatus = "MESSAGE_IN_PROCESS"

	// DeleteStatusNotDeleted indicates messages could not be deleted.
	DeleteStatusNotDeleted DeleteStatus = "MESSAGE_NOT_DELETED"
)

// DeleteStats holds counters for a delete operation.
type DeleteStats struct {
	Processed int `json:"processed"`
	Success   int `json:"success"`
	NotFound  int `json:"notFound"`
	InProcess int `json:"inProcess"`
}

// DeleteResponse contains the result of a message deletion.
type DeleteResponse struct {
	Status DeleteStatus `json:"status"`
	Stats  DeleteStats  `json:"stats"`
}
