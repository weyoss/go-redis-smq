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
	"context"
	"encoding/json"
	"fmt"
	"time"

	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// jobScriptErrorMessages maps each Lua job script's numeric failure codes to
// human-readable messages. The same code means different things in different
// scripts — e.g. complete-job.lua returns -1 when the job is already FAILED,
// while start-job.lua returns -1 when the job is already COMPLETED. A single
// uniform mapping (the previous behaviour) produced misleading error messages
// such as "purge: complete job: job already completed" for a job that had
// actually failed.
var jobScriptErrorMessages = map[scripts.ID]map[int64]string{
	scripts.StartJob: {
		-1: "job already completed",
		-2: "job already failed",
		-3: "job already cancelled",
	},
	scripts.CompleteJob: {
		-1: "job already failed",
		-2: "job already cancelled",
	},
	scripts.FailJob: {
		-1: "job already completed",
		-2: "job already cancelled",
	},
	scripts.CancelJob: {
		-1: "job already completed",
		-2: "job already failed",
	},
	scripts.RecoverStuckJob: {
		-1: "job already completed",
		-2: "job already failed",
		-3: "job already cancelled",
	},
}

// PurgeJobStore handles persistence and lifecycle operations for purge jobs.
// It encapsulates all Redis interactions related to purge job CRUD and state
// transitions.
type PurgeJobStore struct{}

// NewPurgeJobStore creates a new PurgeJobStore.
func NewPurgeJobStore() *PurgeJobStore {
	return &PurgeJobStore{}
}

// Create stores a new purge job and adds it to the pending list.
func (s *PurgeJobStore) Create(ctx context.Context, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.CreateJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.PendingPurgeJobs()},
		[]interface{}{job.ID, mustMarshal(job)},
		"create",
	)
}

// Start marks a job as processing and assigns it to a worker.
func (s *PurgeJobStore) Start(ctx context.Context, jobID, workerID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.StartJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID)},
		[]interface{}{
			jobID, workerID, mustMarshal(job),
			publicqueue.PurgeJobPending.String(), publicqueue.PurgeJobProcessing.String(),
			publicqueue.PurgeJobCompleted.String(), publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
		},
		"start",
	)
}

// Complete marks a job as completed and removes it from the processing list.
func (s *PurgeJobStore) Complete(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.CompleteJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID)},
		[]interface{}{
			jobID, mustMarshal(job),
			publicqueue.PurgeJobProcessing.String(), publicqueue.PurgeJobCompleted.String(),
			publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
		},
		"complete",
	)
}

// Fail marks a job as failed and removes it from the processing list.
func (s *PurgeJobStore) Fail(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.FailJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID)},
		[]interface{}{
			jobID, mustMarshal(job),
			publicqueue.PurgeJobProcessing.String(), publicqueue.PurgeJobCompleted.String(),
			publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
		},
		"fail",
	)
}

// Cancel cancels a job and removes it from all lists.
func (s *PurgeJobStore) Cancel(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.CancelJob,
		[]string{
			keys.System{}.PurgeJobs(), keys.System{}.PendingPurgeJobs(),
			keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID),
		},
		[]interface{}{
			jobID, mustMarshal(job),
			publicqueue.PurgeJobPending.String(), publicqueue.PurgeJobProcessing.String(),
			publicqueue.PurgeJobCompleted.String(), publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
		},
		"cancel",
	)
}

// Recover moves a stuck job from processing back to pending.
func (s *PurgeJobStore) Recover(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.RecoverStuckJob,
		[]string{
			keys.System{}.PurgeJobs(), keys.System{}.PendingPurgeJobs(),
			keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID),
		},
		[]interface{}{
			jobID, mustMarshal(job),
			publicqueue.PurgeJobProcessing.String(), publicqueue.PurgeJobCompleted.String(),
			publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
			"Recovered from worker crash",
		},
		"recover",
	)
}

// Get retrieves a job by ID.
func (s *PurgeJobStore) Get(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
	data, err := redisClient.Client().HGet(ctx, keys.System{}.PurgeJobs(), jobID).Result()
	if err != nil {
		return nil, fmt.Errorf("purge: job not found: %s", jobID)
	}
	var job publicqueue.PurgeJob
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("purge: decode job: %w", err)
	}
	return &job, nil
}

// SaveProgress updates the progress fields of a job (meta.purged, updatedAt)
// via a status-checked Lua script.
//
// It returns (true, nil) if the update was applied, or (false, nil) if the job
// is no longer in PROCESSING state — which means another operation (typically
// Cancel) has already moved it to a terminal state. Callers should treat
// (false, nil) as "stop processing" and return early.
func (s *PurgeJobStore) SaveProgress(ctx context.Context, job *publicqueue.PurgeJob) (bool, error) {
	job.UpdatedAt = time.Now().UnixMilli()

	reply, err := redisClient.Eval(ctx, scripts.SaveJobProgress,
		[]string{keys.System{}.PurgeJobs()},
		[]interface{}{
			job.ID,
			mustMarshal(job),
			publicqueue.PurgeJobProcessing.String(),
		},
	)
	if err != nil {
		return false, fmt.Errorf("purge: save progress: %w", err)
	}

	n, err := redisClient.Int64(reply)
	if err != nil {
		return false, fmt.Errorf("purge: save progress: unexpected reply: %v", reply)
	}

	switch n {
	case 1:
		return true, nil
	case 0:
		return false, fmt.Errorf("purge: job not found for save progress")
	case -1:
		return false, nil
	default:
		return false, fmt.Errorf("purge: save progress: unexpected reply: %d", n)
	}
}

// IsCanceled checks if a job is currently cancelled.
func (s *PurgeJobStore) IsCanceled(ctx context.Context, jobID string) (bool, error) {
	job, err := s.Get(ctx, jobID)
	if err != nil {
		return false, err
	}
	return job.Status == publicqueue.PurgeJobCanceled, nil
}

// runJobScript executes a purge job Lua script and interprets the result.
func runJobScript(ctx context.Context, id scripts.ID, keys []string, args []interface{}, operation string) error {
	reply, err := redisClient.Eval(ctx, id, keys, args...)
	if err != nil {
		return fmt.Errorf("purge: %s job: %w", operation, err)
	}

	n, err := redisClient.Int64(reply)
	if err != nil {
		return fmt.Errorf("purge: %s job: unexpected reply: %v", operation, reply)
	}

	switch n {
	case 1, 2:
		// 1 = success; 2 = already in the desired state. Both are idempotent
		// successes from the caller's perspective.
		return nil
	case 0:
		return fmt.Errorf("purge: %s job: not found", operation)
	}

	if msgs, ok := jobScriptErrorMessages[id]; ok {
		if msg, ok := msgs[n]; ok {
			return fmt.Errorf("purge: %s job: %s", operation, msg)
		}
	}
	return fmt.Errorf("purge: %s job: unexpected reply: %d", operation, n)
}

// mustMarshal marshals any value to a JSON string, panicking on error.
func mustMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("purge: marshal: %v", err))
	}
	return string(data)
}
