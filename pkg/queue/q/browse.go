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
	BrowseDeadLettered BrowseFilter = iota // 0 - Failed messages
	BrowseAcknowledged                     // 1 - Successfully processed messages
	BrowseScheduled                        // 2 - Messages scheduled for future delivery
	BrowsePending                          // 3 - Messages waiting to be consumed
	BrowsePublished                        // 4 - All messages in the queue
)

// BrowseParams controls pagination and ordering.
type BrowseParams struct {
	Filter BrowseFilter
	Offset int64
	Count  int64 // 0 means use default (100)
}

// BrowseResult holds a page of message IDs with pagination info.
type BrowseResult struct {
	IDs     []string `json:"ids"`
	Total   int64    `json:"total"`
	Offset  int64    `json:"offset"`
	Count   int64    `json:"count"`
	HasMore bool     `json:"hasMore"`
}
