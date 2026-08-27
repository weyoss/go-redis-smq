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

func create(ctx context.Context, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.CreateJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.PendingPurgeJobs()},
		[]interface{}{job.ID, mustMarshal(job)},
		"create",
	)
}

func start(ctx context.Context, jobID, workerID string, job *publicqueue.PurgeJob) error {
	return runJobScript(ctx, scripts.StartJob,
		[]string{keys.System{}.PurgeJobs(), keys.System{}.ActivePurgeJobs(), keys.System{}.JobWorker(jobID)},
		[]interface{}{
			jobID, workerID, mustMarshal(job),
			(publicqueue.PurgeJobPending).String(), publicqueue.PurgeJobProcessing.String(),
			publicqueue.PurgeJobCompleted.String(), publicqueue.PurgeJobFailed.String(), publicqueue.PurgeJobCanceled.String(),
		},
		"start",
	)
}

func complete(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
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

func fail(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
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

func cancel(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
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

func recoverJob(ctx context.Context, jobID string, job *publicqueue.PurgeJob) error {
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

func getJob(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
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

func save(ctx context.Context, job *publicqueue.PurgeJob) error {
	job.UpdatedAt = time.Now().UnixMilli()
	return redisClient.Client().HSet(ctx, keys.System{}.PurgeJobs(), job.ID, mustMarshal(job)).Err()
}

func isCanceled(ctx context.Context, jobID string) (bool, error) {
	job, err := getJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	return job.Status == publicqueue.PurgeJobCanceled, nil
}

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

func mustMarshal(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("purge: marshal: %v", err))
	}
	return string(data)
}
