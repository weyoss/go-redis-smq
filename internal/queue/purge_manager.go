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
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/config"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// PurgeManager orchestrates queue purge jobs. It coordinates locking,
// job lifecycle, and message deletion.
type PurgeManager struct {
	state        *State
	messageStore *internalMessage.Store
	jobStore     *PurgeJobStore
	workerID     string
	log          *slog.Logger
}

// NewPurgeManager creates a new PurgeManager with the given state and message store.
func NewPurgeManager(state *State, messageStore *internalMessage.Store) *PurgeManager {
	if state == nil {
		panic("purge: state is required")
	}
	if messageStore == nil {
		panic("purge: messageStore is required")
	}
	id := uuid.New().String()
	return &PurgeManager{
		state:        state,
		messageStore: messageStore,
		jobStore:     NewPurgeJobStore(),
		workerID:     id,
		log:          logger.New("purge", "manager", id),
	}
}

// Enqueue creates a new purge job and locks the queue.
func (pm *PurgeManager) Enqueue(ctx context.Context, queueParams *publicqueue.Params, filter publicqueue.BrowseFilter) (string, error) {
	if err := pm.validateFilter(filter); err != nil {
		return "", err
	}

	jobID := uuid.New().String()
	reason := publicqueue.TransitionReason(publicqueue.ReasonPurgeStart)
	lockOpts := &publicqueue.StateTransitionOptions{
		Description: ptr("Queue is being purged"),
	}
	if _, err := pm.state.acquireLock(ctx, queueParams, publicqueue.LockOwnerPurgeJob, jobID, reason, lockOpts); err != nil {
		return "", fmt.Errorf("purge: lock queue: %w", err)
	}

	job := newPurgeJob(jobID, queueParams, filter)
	if err := pm.jobStore.Create(ctx, job); err != nil {
		pm.unlockQueue(ctx, queueParams, jobID, publicqueue.PurgeJobFailed, err.Error())
		return "", fmt.Errorf("purge: enqueue job: %w", err)
	}

	return jobID, nil
}

// Get returns a purge job by ID.
func (pm *PurgeManager) Get(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
	return pm.jobStore.Get(ctx, jobID)
}

// Cancel cancels a pending or processing purge job.
func (pm *PurgeManager) Cancel(ctx context.Context, queueParams *publicqueue.Params, jobID string) error {
	job, err := pm.jobStore.Get(ctx, jobID)
	if err != nil {
		return fmt.Errorf("purge: get job: %w", err)
	}

	job.Status = publicqueue.PurgeJobCanceled
	job.CompletedAt = time.Now().UnixMilli()
	if err := pm.jobStore.Cancel(ctx, jobID, job); err != nil {
		return fmt.Errorf("purge: cancel job: %w", err)
	}

	pm.unlockQueue(ctx, queueParams, jobID, publicqueue.PurgeJobCanceled, "Purge job cancelled")
	return nil
}

// execute processes a single job from start to finish.
func (pm *PurgeManager) execute(ctx context.Context, jobID string) {
	job, err := pm.jobStore.Get(ctx, jobID)
	if err != nil {
		pm.log.Error("get job failed", "jobID", jobID, "error", err)
		return
	}

	queueParams := job.Payload.Queue

	job.Status = publicqueue.PurgeJobProcessing
	job.StartedAt = time.Now().UnixMilli()
	if err := pm.jobStore.Start(ctx, jobID, pm.workerID, job); err != nil {
		pm.log.Error("start job failed", "jobID", jobID, "error", err)
		pm.failJob(ctx, job, queueParams, err.Error())
		return
	}

	purged, err := pm.purgeMessages(ctx, job)
	if err != nil {
		pm.log.Error("job failed", "jobID", jobID, "error", err)
		pm.failJob(ctx, job, queueParams, err.Error())
		return
	}

	// Check if the job was cancelled during processing.
	canceled, err := pm.jobStore.IsCanceled(ctx, jobID)
	if err != nil {
		pm.log.Error("check canceled failed", "jobID", jobID, "error", err)
	}
	if canceled {
		pm.log.Info("job was cancelled", "jobID", jobID)
		return
	}

	job.Status = publicqueue.PurgeJobCompleted
	if job.Meta == nil {
		job.Meta = &publicqueue.PurgeJobMeta{}
	}
	job.Meta.Purged = purged
	job.CompletedAt = time.Now().UnixMilli()
	if err := pm.jobStore.Complete(ctx, jobID, job); err != nil {
		pm.log.Error("complete job failed", "jobID", jobID, "error", err)
	}
	pm.unlockQueue(ctx, queueParams, jobID, publicqueue.PurgeJobCompleted, "Purge completed")
}

// purgeMessages deletes messages in batches for the given job.
func (pm *PurgeManager) purgeMessages(ctx context.Context, job *publicqueue.PurgeJob) (int64, error) {
	qKey := keys.Queue{Namespace: job.Payload.Queue.NS(), Name: job.Payload.Queue.Name()}
	categoryKey := resolveCategoryKey(job.Payload.MessageType, qKey)

	var purged int64
	for {
		select {
		case <-ctx.Done():
			return purged, ctx.Err()
		default:
		}

		canceled, err := pm.jobStore.IsCanceled(ctx, job.ID)
		if err != nil {
			return purged, fmt.Errorf("check canceled: %w", err)
		}
		if canceled {
			return purged, nil
		}

		ids, err := fetchBatch(ctx, categoryKey, job.BatchSize)
		if err != nil {
			return purged, fmt.Errorf("fetch batch: %w", err)
		}
		if len(ids) == 0 {
			return purged, nil
		}

		result, err := pm.messageStore.DeleteMessages(ctx, ids, job.ID)
		if err != nil {
			return purged, fmt.Errorf("delete messages: %w", err)
		}

		purged += int64(result.Stats.Success)

		if err := trimCategory(ctx, categoryKey, int64(len(ids))); err != nil {
			return purged, fmt.Errorf("trim category: %w", err)
		}

		if job.Meta == nil {
			job.Meta = &publicqueue.PurgeJobMeta{}
		}
		job.Meta.Purged = purged
		if err := pm.jobStore.Save(ctx, job); err != nil {
			pm.log.Error("save job progress failed", "jobID", job.ID, "error", err)
		}

		if len(ids) == job.BatchSize {
			select {
			case <-ctx.Done():
				return purged, ctx.Err()
			case <-time.After(job.BatchDelay()):
			}
		}
	}
}

// failJob marks a job as failed and unlocks the queue.
func (pm *PurgeManager) failJob(ctx context.Context, job *publicqueue.PurgeJob, queueParams *publicqueue.Params, errMsg string) {
	job.Status = publicqueue.PurgeJobFailed
	job.Error = errMsg
	job.CompletedAt = time.Now().UnixMilli()
	if err := pm.jobStore.Fail(ctx, job.ID, job); err != nil {
		pm.log.Error("failed to mark job as failed", "jobID", job.ID, "error", err)
	}
	pm.unlockQueue(ctx, queueParams, job.ID, publicqueue.PurgeJobFailed, errMsg)
}

// unlockQueue releases the lock held by a purge job.
func (pm *PurgeManager) unlockQueue(ctx context.Context, queueParams *publicqueue.Params, jobID string, status publicqueue.PurgeJobStatus, description string) {
	var reason publicqueue.TransitionReason
	switch status {
	case publicqueue.PurgeJobCompleted:
		reason = publicqueue.TransitionReason(publicqueue.ReasonPurgeComplete)
	case publicqueue.PurgeJobFailed:
		reason = publicqueue.TransitionReason(publicqueue.ReasonPurgeFail)
	case publicqueue.PurgeJobCanceled:
		reason = publicqueue.TransitionReason(publicqueue.ReasonPurgeCancel)
	default:
		reason = publicqueue.TransitionReason(publicqueue.ReasonPurgeComplete)
	}
	opts := &publicqueue.StateTransitionOptions{
		Description: &description,
	}
	if _, err := pm.state.releaseLock(ctx, queueParams, publicqueue.LockOwnerPurgeJob, jobID, reason, opts); err != nil {
		pm.log.Error("unlock queue failed", "jobID", jobID, "error", err)
	}
}

// validateFilter checks that the message category is eligible for purge.
func (pm *PurgeManager) validateFilter(filter publicqueue.BrowseFilter) error {
	cfg := config.Get()
	switch filter {
	case publicqueue.BrowseAcknowledged:
		if !cfg.MessageAudit.AcknowledgedMessages.Enabled {
			return fmt.Errorf("purge: %w", publicqueue.ErrAuditDisabled)
		}
	case publicqueue.BrowseDeadLettered:
		if !cfg.MessageAudit.DeadLetteredMessages.Enabled {
			return fmt.Errorf("purge: %w", publicqueue.ErrAuditDisabled)
		}
	}
	return nil
}

// recoverStuckJobs checks for jobs whose worker is no longer alive and re-queues them.
func (pm *PurgeManager) recoverStuckJobs(ctx context.Context) {
	jobIDs, err := redisClient.LoadSetMembers(ctx, keys.System{}.ActivePurgeJobs(), "processing purge jobs")
	if err != nil {
		pm.log.Debug("failed to load processing purge jobs", "error", err)
		return
	}
	for _, jobID := range jobIDs {
		if IsWorkerAlive(ctx, jobID) {
			continue
		}
		job, err := pm.jobStore.Get(ctx, jobID)
		if err != nil {
			continue
		}
		pm.log.Warn("recovering stuck job", "jobID", jobID)
		job.Status = publicqueue.PurgeJobPending
		job.Error = "Recovered from worker crash"
		job.UpdatedAt = time.Now().UnixMilli()
		if err := pm.jobStore.Recover(ctx, jobID, job); err != nil {
			pm.log.Error("recover stuck job failed", "jobID", jobID, "error", err)
		}
	}
}

// newPurgeJob creates a new pending purge job.
func newPurgeJob(jobID string, queueParams *publicqueue.Params, filter publicqueue.BrowseFilter) *publicqueue.PurgeJob {
	return &publicqueue.PurgeJob{
		ID: jobID,
		Payload: publicqueue.PurgeJobPayload{
			Queue:       queueParams.Clone(),
			MessageType: filter,
		},
		Status:    publicqueue.PurgeJobPending,
		BatchSize: defaultPurgeBatchSize,
		DelayMs:   defaultPurgeBatchDelay.Milliseconds(),
		CreatedAt: time.Now().UnixMilli(),
		Meta:      &publicqueue.PurgeJobMeta{Purged: 0},
	}
}

// resolveCategoryKey maps a browse filter to its Redis key.
func resolveCategoryKey(filter publicqueue.BrowseFilter, qKey keys.Queue) string {
	switch filter {
	case publicqueue.BrowsePending:
		return qKey.Pending()
	case publicqueue.BrowseScheduled:
		return qKey.Scheduled()
	case publicqueue.BrowseAcknowledged:
		return qKey.Acknowledged()
	case publicqueue.BrowseDeadLettered:
		return qKey.DeadLetter()
	default:
		return ""
	}
}
