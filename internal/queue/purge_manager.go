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
//
// parsedParams carries both the queue identity and (for Pub/Sub queues)
// the target consumer group. The group is written into the persisted
// payload so the eventual purgeMessages call can resolve the correct
// per-group storage key. See resolveCategoryKey for the dispatch rules.
func (pm *PurgeManager) Enqueue(
	ctx context.Context,
	parsedParams publicqueue.ParsedParams,
	filter publicqueue.BrowseFilter,
) (string, error) {
	if parsedParams.QueueParams.Name() == "" {
		return "", publicqueue.ErrNameRequired
	}

	if err := pm.validateFilter(filter); err != nil {
		return "", err
	}

	jobID := uuid.New().String()
	reason := publicqueue.TransitionReason(publicqueue.ReasonPurgeStart)
	lockOpts := &publicqueue.StateTransitionOptions{
		Description: ptr("Queue is being purged"),
	}
	if _, err := pm.state.acquireLock(
		ctx, &parsedParams.QueueParams,
		publicqueue.LockOwnerPurgeJob, jobID, reason, lockOpts,
	); err != nil {
		return "", fmt.Errorf("purge: lock queue: %w", err)
	}

	job := newPurgeJob(jobID, parsedParams, filter)
	if err := pm.jobStore.Create(ctx, job); err != nil {
		pm.unlockQueue(ctx, &parsedParams.QueueParams, jobID, publicqueue.PurgeJobFailed, err.Error())
		return "", fmt.Errorf("purge: enqueue job: %w", err)
	}

	return jobID, nil
}

// Get returns a purge job by ID.
func (pm *PurgeManager) Get(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
	return pm.jobStore.Get(ctx, jobID)
}

// Cancel cancels a pending or processing purge job.
//
// queueParams is the queue-level identity only — the lock is acquired at
// the queue level, not per consumer group, so the caller's *Params is
// sufficient for releasing it. The job's own payload may carry a group
// ID; that has no bearing on the unlock path.
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

	// queueParams is the queue-level identity needed by state transitions.
	// The group ID, if present, is part of Payload.Queue and is consumed
	// by purgeMessages via resolveCategoryKey.
	queueParams := &job.Payload.Queue.QueueParams

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
//
// The category key is derived from three inputs (see resolveCategoryKey):
// the filter, the queue type, and the payload's group ID. Loading the
// queue properties is required because priority queues store pending
// messages in a zset (Priority / PriorityWithGroup) rather than a list
// (Pending / PendingWithGroup) — reading the wrong structure returns
// nothing, so a priority pending purge would silently be a no-op.
//
// For Pub/Sub queues the group ID is essential: messages live in
// per-group PendingWithGroup lists, and the queue-level Pending list
// is empty. Reading it without the group would return nothing.
func (pm *PurgeManager) purgeMessages(ctx context.Context, job *publicqueue.PurgeJob) (int64, error) {
	parsed := job.Payload.Queue
	qKey := keys.Queue{
		Namespace: parsed.QueueParams.NS(),
		Name:      parsed.QueueParams.Name(),
	}

	props, err := NewStore(nil).Load(ctx, &parsed.QueueParams)
	if err != nil {
		return 0, fmt.Errorf("load queue properties: %w", err)
	}

	categoryKey := resolveCategoryKey(job.Payload.MessageType, qKey, props.Type, parsed.GroupID)
	if categoryKey == "" {
		return 0, fmt.Errorf("purge: unresolvable category for filter %d", job.Payload.MessageType)
	}

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

		// DeleteMessages is atomic per message: each successfully-deleted
		// ID is LREM'd/ZREM'd from the category by delete-message.lua.
		//
		// The only IDs that survive in the category after that are
		// orphan IDs — message hash gone (counted as NotFound by
		// GetMessagesLenient's pre-skip). Left unchecked, orphan IDs
		// would be re-fetched on every iteration and the loop would
		// never make progress. Force-remove them now.
		//
		// InProcess IDs are NOT in the category (the consumer popped
		// them out during a fetch/delete race) — no cleanup needed; the
		// message resolves through the normal retry/ack path. That's why
		// the condition keys only on NotFound.
		if result.Stats.NotFound > 0 {
			if err := forceRemoveFromCategory(ctx, categoryKey, ids); err != nil {
				return purged, fmt.Errorf("clean orphans from category: %w", err)
			}
		}

		if job.Meta == nil {
			job.Meta = &publicqueue.PurgeJobMeta{}
		}
		job.Meta.Purged = purged

		updated, err := pm.jobStore.SaveProgress(ctx, job)
		if err != nil {
			pm.log.Error("save job progress failed", "jobID", job.ID, "error", err)
		} else if !updated {
			pm.log.Info("job transitioned out of PROCESSING; stopping batch loop",
				"jobID", job.ID, "purgedSoFar", purged)
			return purged, nil
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
func (pm *PurgeManager) failJob(
	ctx context.Context,
	job *publicqueue.PurgeJob,
	queueParams *publicqueue.Params,
	errMsg string,
) {
	job.Status = publicqueue.PurgeJobFailed
	job.Error = errMsg
	job.CompletedAt = time.Now().UnixMilli()
	if err := pm.jobStore.Fail(ctx, job.ID, job); err != nil {
		pm.log.Error("failed to mark job as failed", "jobID", job.ID, "error", err)
	}
	pm.unlockQueue(ctx, queueParams, job.ID, publicqueue.PurgeJobFailed, errMsg)
}

// unlockQueue releases the lock held by a purge job.
//
// queueParams is the queue-level identity. The lock was acquired at the
// queue level (regardless of any group carried in the payload), so the
// unlock path needs only {name, ns}.
func (pm *PurgeManager) unlockQueue(
	ctx context.Context,
	queueParams *publicqueue.Params,
	jobID string,
	status publicqueue.PurgeJobStatus,
	description string,
) {
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

// validateFilter rejects filters that cannot be purged.
//
// Purgeable filters are Pending, Scheduled, Acknowledged, and
// DeadLettered — each maps to a Redis structure holding that category's
// message IDs (see resolveCategoryKey). BrowsePublished maps to the
// queue's all-messages index, not a single category: an item in that
// list is a secondary index into a primary category, so "purge
// published" would mean "delete every message regardless of state" —
// a different operation, not supported by the purge-job model.
//
// Rejecting unsupported filters here (before lock acquisition and
// enqueue) avoids the pre-fix flow: lock the queue → enqueue the job
// → the worker's purgeMessages hits resolveCategoryKey's empty-string
// return → the job fails asynchronously, and the queue stays LOCKED
// until the failure path or the orphaned-lock recoverer unlocks it.
func (pm *PurgeManager) validateFilter(filter publicqueue.BrowseFilter) error {
	cfg := config.Get()
	switch filter {
	case publicqueue.BrowsePending, publicqueue.BrowseScheduled:
		// Always purgeable — no audit gate.
		return nil
	case publicqueue.BrowseAcknowledged:
		if !cfg.MessageAudit.AcknowledgedMessages.Enabled {
			return fmt.Errorf("purge: %w", publicqueue.ErrAuditDisabled)
		}
		return nil
	case publicqueue.BrowseDeadLettered:
		if !cfg.MessageAudit.DeadLetteredMessages.Enabled {
			return fmt.Errorf("purge: %w", publicqueue.ErrAuditDisabled)
		}
		return nil
	default:
		return fmt.Errorf("purge: filter %d is not purgeable", filter)
	}
}

// recoverStuckJobs checks for jobs whose worker is no longer alive and re-queues them.
func (pm *PurgeManager) recoverStuckJobs(ctx context.Context) {
	jobIDs, err := redisClient.LoadListMembers(ctx,
		keys.System{}.ActivePurgeJobs(), "processing purge jobs")
	if err != nil {
		pm.log.Error("failed to load processing purge jobs", "error", err)
		return
	}

	// Defensive dedup: start-job.lua normalises the list to one entry per job,
	// but a snapshot taken mid-normalisation could still see duplicates.
	seen := make(map[string]struct{}, len(jobIDs))

	for _, jobID := range jobIDs {
		if _, dup := seen[jobID]; dup {
			continue
		}
		seen[jobID] = struct{}{}

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
//
// parsedParams is written into the payload verbatim — including the group
// ID, if any. This is what purgeMessages later reads to resolve the
// per-group category key. Passing the group through unmodified is what
// makes PurgeQueueWithGroup work end-to-end.
func newPurgeJob(
	jobID string,
	parsedParams publicqueue.ParsedParams,
	filter publicqueue.BrowseFilter,
) *publicqueue.PurgeJob {
	return &publicqueue.PurgeJob{
		ID: jobID,
		Payload: publicqueue.PurgeJobPayload{
			Queue:       parsedParams,
			MessageType: filter,
		},
		Status:    publicqueue.PurgeJobPending,
		BatchSize: defaultPurgeBatchSize,
		DelayMs:   defaultPurgeBatchDelay.Milliseconds(),
		CreatedAt: time.Now().UnixMilli(),
		Meta:      &publicqueue.PurgeJobMeta{Purged: 0},
	}
}
