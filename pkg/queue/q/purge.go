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

import (
	"encoding/json"
	"strconv"
	"time"
)

// PurgeJobStatus represents the lifecycle of a purge background job.
type PurgeJobStatus int

const (
	PurgeJobPending    PurgeJobStatus = iota // 0
	PurgeJobProcessing                       // 1
	PurgeJobCompleted                        // 2
	PurgeJobFailed                           // 3
	PurgeJobCanceled                         // 4
)

func (s PurgeJobStatus) String() string { return strconv.Itoa(int(s)) }

func (s PurgeJobStatus) MarshalJSON() ([]byte, error) { return json.Marshal(int(s)) }
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
	Queue       *QueueParams `json:"queue"`
	MessageType BrowseFilter `json:"messageType"`
}

// PurgeJobMeta holds job‑specific metadata.
type PurgeJobMeta struct {
	Purged int64 `json:"purged"`
}

// PurgeJob represents a queue purge background job.
type PurgeJob struct {
	ID          string          `json:"id"`
	Payload     PurgeJobPayload `json:"payload"`
	Status      PurgeJobStatus  `json:"status"`
	CreatedAt   int64           `json:"createdAt"`
	UpdatedAt   int64           `json:"updatedAt,omitempty"`
	StartedAt   int64           `json:"startedAt,omitempty"`
	CompletedAt int64           `json:"completedAt,omitempty"`
	BatchSize   int             `json:"batchSize,omitempty"`
	DelayMs     int64           `json:"delay,omitempty"`
	Error       string          `json:"error,omitempty"`
	Meta        *PurgeJobMeta   `json:"meta,omitempty"`
}

// QueueParams returns the queue parameters from the payload.
func (j *PurgeJob) QueueParams() *QueueParams {
	return j.Payload.Queue
}

// BatchDelay returns the delay as a time.Duration.
func (j *PurgeJob) BatchDelay() time.Duration {
	return time.Duration(j.DelayMs) * time.Millisecond
}
