/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package queue

import "encoding/json"

// ParsedParams pairs a queue with its effective consumer group ID.
//
// It is the Go equivalent of TypeScript's IQueueParsedParams interface,
// and the JSON wire format is identical:
//
//	{
//	  "queueParams": { "name": "orders", "ns": "default" },
//	  "groupId": "email-service"
//	}
//
// For point-to-point queues and for Pub/Sub queues before a group is
// resolved, GroupID is empty and serializes as null on the wire, matching
// the TypeScript interface's `groupId: string | null`.
type ParsedParams struct {
	QueueParams Params
	GroupID     string
}

// MarshalJSON implements custom JSON marshaling for cross-language
// compatibility with the TypeScript IQueueParsedParams format.
//
// An empty GroupID serializes as null (not ""), matching the TypeScript
// interface's `groupId: string | null`. This lets TypeScript subscribers
// that check `queue.groupId === null` behave correctly when the event
// originated from a Go process.
func (p ParsedParams) MarshalJSON() ([]byte, error) {
	var gid interface{}
	if p.GroupID != "" {
		gid = p.GroupID
	}
	return json.Marshal(&struct {
		QueueParams Params      `json:"queueParams"`
		GroupID     interface{} `json:"groupId"`
	}{
		QueueParams: p.QueueParams,
		GroupID:     gid,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for cross-language
// compatibility. Both a JSON null and a missing groupId field map to the
// empty string.
func (p *ParsedParams) UnmarshalJSON(data []byte) error {
	var aux struct {
		QueueParams Params  `json:"queueParams"`
		GroupID     *string `json:"groupId"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	p.QueueParams = aux.QueueParams
	if aux.GroupID != nil {
		p.GroupID = *aux.GroupID
	} else {
		p.GroupID = ""
	}
	return nil
}
