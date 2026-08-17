/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package q

// BrowseFilter defines which message category to browse.
type BrowseFilter int

const (
	// BrowseDeadLettered returns messages that failed permanently.
	BrowseDeadLettered BrowseFilter = iota // 0

	// BrowseAcknowledged returns messages that were successfully processed.
	// Requires message audit to be enabled.
	BrowseAcknowledged // 1

	// BrowseScheduled returns messages waiting for future delivery.
	BrowseScheduled // 2

	// BrowsePending returns messages waiting to be consumed.
	BrowsePending // 3

	// BrowsePublished returns all messages currently stored in the queue.
	BrowsePublished // 4
)

// BrowseParams controls pagination and filtering when browsing messages.
type BrowseParams struct {
	// Filter determines which message category to browse.
	Filter BrowseFilter

	// Offset is the number of messages to skip before returning results.
	Offset int64

	// Count is the maximum number of message IDs to return per page.
	// A value of zero means the default count (100) is used.
	Count int64
}

// BrowseResult holds a page of message IDs with pagination information.
type BrowseResult struct {
	// IDs contains the message IDs in the current page.
	IDs []string `json:"ids"`

	// Total is the total number of messages in the selected category.
	Total int64 `json:"total"`

	// Offset is the starting offset of this page.
	Offset int64 `json:"offset"`

	// Count is the number of IDs returned in this page.
	Count int64 `json:"count"`

	// HasMore indicates whether there are additional pages after this one.
	HasMore bool `json:"hasMore"`
}
