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

import (
	"encoding/json"
	"strconv"
	"time"
)

// PurgeJobStatus represents the lifecycle of a purge background job.
// Serialized as integer in JSON for Lua script compatibility with TypeScript.
type PurgeJobStatus int

const (
	// PurgeJobPending is the status of a job waiting to be processed.
	PurgeJobPending PurgeJobStatus = iota

	// PurgeJobProcessing is the status of a job currently being processed.
	PurgeJobProcessing

	// PurgeJobCompleted is the status of a successfully completed job.
	PurgeJobCompleted

	// PurgeJobFailed is the status of a failed job.
	PurgeJobFailed

	// PurgeJobCanceled is the status of a cancelled job.
	PurgeJobCanceled
)

// String returns the integer representation as a string.
func (s PurgeJobStatus) String() string { return strconv.Itoa(int(s)) }

// MarshalJSON serializes PurgeJobStatus as an integer.
func (s PurgeJobStatus) MarshalJSON() ([]byte, error) { return json.Marshal(int(s)) }

// UnmarshalJSON deserializes PurgeJobStatus from an integer.
func (s *PurgeJobStatus) UnmarshalJSON(data []byte) error {
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*s = PurgeJobStatus(n)
	return nil
}

// PurgeJobPayload contains the queue and message type for a purge job.
type PurgeJobPayload struct {
	// Queue is the target queue for the purge job.
	Queue *Params `json:"queue"`

	// MessageType is the category of messages to purge.
	MessageType BrowseFilter `json:"messageType"`
}

// PurgeJobMeta holds job-specific metadata.
type PurgeJobMeta struct {
	// Purged is the number of messages successfully purged so far.
	Purged int64 `json:"purged"`
}

// PurgeJob represents a queue purge background job.
type PurgeJob struct {
	// ID is the unique job identifier.
	ID string `json:"id"`

	// Payload contains the target queue and message category.
	Payload PurgeJobPayload `json:"payload"`

	// Status is the current lifecycle status of the job.
	Status PurgeJobStatus `json:"status"`

	// CreatedAt is the job creation timestamp.
	CreatedAt int64 `json:"createdAt"`

	// UpdatedAt is the last update timestamp.
	UpdatedAt int64 `json:"updatedAt,omitempty"`

	// StartedAt is the timestamp when processing began.
	StartedAt int64 `json:"startedAt,omitempty"`

	// CompletedAt is the timestamp when processing completed.
	CompletedAt int64 `json:"completedAt,omitempty"`

	// BatchSize is the number of messages to delete per batch.
	BatchSize int `json:"batchSize,omitempty"`

	// DelayMs is the delay between batches in milliseconds.
	DelayMs int64 `json:"delay,omitempty"`

	// Error contains the error message if the job failed.
	Error string `json:"error,omitempty"`

	// Meta holds job metadata.
	Meta *PurgeJobMeta `json:"meta,omitempty"`
}

// QueueParams returns the queue parameters from the payload.
func (j *PurgeJob) QueueParams() *Params {
	return j.Payload.Queue
}

// BatchDelay returns the delay as a time.Duration.
func (j *PurgeJob) BatchDelay() time.Duration {
	return time.Duration(j.DelayMs) * time.Millisecond
}
