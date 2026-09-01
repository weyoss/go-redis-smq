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

// Save updates the job in Redis.
func (s *PurgeJobStore) Save(ctx context.Context, job *publicqueue.PurgeJob) error {
	job.UpdatedAt = time.Now().UnixMilli()
	return redisClient.Client().HSet(ctx, keys.System{}.PurgeJobs(), job.ID, mustMarshal(job)).Err()
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
		return fmt.Errorf("purge: unexpected %s reply: %v", operation, reply)
	}

	switch n {
	case 1, 2:
		return nil
	case 0:
		return fmt.Errorf("purge: job not found for %s", operation)
	case -1:
		return fmt.Errorf("purge: job already completed")
	case -2:
		return fmt.Errorf("purge: job already failed")
	case -3:
		return fmt.Errorf("purge: job already cancelled")
	default:
		return fmt.Errorf("purge: unexpected %s reply: %d", operation, n)
	}
}

// mustMarshal marshals any value to a JSON string, panicking on error.
func mustMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("purge: marshal: %v", err))
	}
	return string(data)
}
