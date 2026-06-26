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
	"sync"
	"time"

	"github.com/google/uuid"
	rdb "github.com/redis/go-redis/v9"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
	"github.com/weyoss/go-redis-smq/pkg/config"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

const (
	defaultPurgeBatchSize   = 1000
	defaultPurgeBatchDelay  = 5 * time.Second
	workerHeartbeatInterval = 10 * time.Second
	workerHeartbeatTTL      = 30 * time.Second
)

type PurgeManager struct {
	state        *State
	messageStore *internalMessage.Store
	workerID     string
	log          *slog.Logger
}

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
		workerID:     id,
		log:          logger.New("purge", "worker", id),
	}
}

var (
	purgeWorkerMu      sync.Mutex
	purgeWorkerCancel  context.CancelFunc
	purgeWorkerClient  *rdb.Client
	purgeWorkerDone    chan struct{}
	purgeWorkerStarted bool
)

// StartPurgeWorker creates a dedicated Redis client for blocking commands.
// On shutdown, closing this client terminates the TCP connection,
// immediately unblocking any pending BRPopLPush.
// Safe to call multiple times — subsequent calls are no-ops.
func StartPurgeWorker(ctx context.Context) func() {
	purgeWorkerMu.Lock()
	defer purgeWorkerMu.Unlock()

	if purgeWorkerStarted {
		return func() {
			purgeWorkerMu.Lock()
			defer purgeWorkerMu.Unlock()
			stopPurgeWorker()
		}
	}

	pm := NewManager().Purge()

	// Create a dedicated Redis client for the purge worker.
	// BRPopLPush blocks the connection and does not respect context cancellation,
	// so we need a separate client that can be closed to unblock it.
	opts := redisClient.Client().Options()
	purgeWorkerClient = rdb.NewClient(opts)

	workerCtx, cancel := context.WithCancel(ctx)
	purgeWorkerCancel = cancel
	purgeWorkerDone = make(chan struct{})

	go func() {
		defer close(purgeWorkerDone)
		defer func() {
			if purgeWorkerClient != nil {
				purgeWorkerClient.Close()
			}
		}()
		pm.work(workerCtx, purgeWorkerClient)
	}()

	purgeWorkerStarted = true

	return func() {
		purgeWorkerMu.Lock()
		defer purgeWorkerMu.Unlock()
		stopPurgeWorker()
	}
}

func stopPurgeWorker() {
	// 1. Close the dedicated client first — unblocks any pending BRPopLPush
	if purgeWorkerClient != nil {
		purgeWorkerClient.Close()
		purgeWorkerClient = nil
	}
	// 2. Cancel context — ensures the error handler sees ctx.Done() immediately
	if purgeWorkerCancel != nil {
		purgeWorkerCancel()
		purgeWorkerCancel = nil
	}
	// 3. Wait for the goroutine to fully exit
	if purgeWorkerDone != nil {
		<-purgeWorkerDone
		purgeWorkerDone = nil
	}
	purgeWorkerStarted = false
}

func (pm *PurgeManager) Enqueue(ctx context.Context, queueParams *q.QueueParams, filter q.BrowseFilter) (string, error) {
	if err := pm.validateFilter(filter); err != nil {
		return "", err
	}

	jobID := uuid.New().String()

	reason := q.QueueStateTransitionReason(q.ReasonPurgeStart)
	lockOpts := &q.StateTransitionOptions{
		Description: ptr("Queue is being purged"),
	}
	if _, err := pm.state.acquireLock(ctx, queueParams, q.LockOwnerPurgeJob, jobID, reason, lockOpts); err != nil {
		return "", fmt.Errorf("purge: lock queue: %w", err)
	}

	job := newPurgeJob(jobID, queueParams, filter)

	if err := create(ctx, job); err != nil {
		pm.unlockQueue(ctx, queueParams, jobID, q.PurgeJobFailed, err.Error())
		return "", fmt.Errorf("purge: enqueue job: %w", err)
	}

	return jobID, nil
}

func (pm *PurgeManager) Get(ctx context.Context, jobID string) (*q.PurgeJob, error) {
	return getJob(ctx, jobID)
}

func (pm *PurgeManager) Cancel(ctx context.Context, queueParams *q.QueueParams, jobID string) error {
	job, err := getJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("purge: get job: %w", err)
	}

	job.Status = q.PurgeJobCanceled
	job.CompletedAt = time.Now().UnixMilli()

	if err := cancel(ctx, jobID, job); err != nil {
		return fmt.Errorf("purge: cancel job: %w", err)
	}

	pm.unlockQueue(ctx, queueParams, jobID, q.PurgeJobCanceled, "Purge job cancelled")
	return nil
}

func (pm *PurgeManager) work(ctx context.Context, redisClient *rdb.Client) {
	pm.recoverStuckJobs(ctx)
	go pm.heartbeatLoop(ctx)

	pm.log.Info("purge worker started")

	for {
		select {
		case <-ctx.Done():
			pm.log.Info("purge worker stopped")
			return
		default:
		}

		jobID, err := acquire(ctx, redisClient)
		if err != nil {
			pm.log.Debug("acquire job failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if jobID == "" {
			continue
		}

		pm.execute(ctx, jobID)
	}
}

func (pm *PurgeManager) execute(ctx context.Context, jobID string) {
	job, err := getJob(ctx, jobID)
	if err != nil {
		pm.log.Error("get job failed", "jobID", jobID, "error", err)
		return
	}

	queueParams := job.Payload.Queue

	job.Status = q.PurgeJobProcessing
	job.StartedAt = time.Now().UnixMilli()

	if err := start(ctx, jobID, pm.workerID, job); err != nil {
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

	job.Status = q.PurgeJobCompleted
	if job.Meta == nil {
		job.Meta = &q.PurgeJobMeta{}
	}
	job.Meta.Purged = purged
	job.CompletedAt = time.Now().UnixMilli()
	if err := complete(ctx, jobID, job); err != nil {
		pm.log.Error("complete job failed", "jobID", jobID, "error", err)
	}
	pm.unlockQueue(ctx, queueParams, jobID, q.PurgeJobCompleted, "Purge completed")
}

func (pm *PurgeManager) purgeMessages(ctx context.Context, job *q.PurgeJob) (int64, error) {
	qKey := keys.Queue{Namespace: job.Payload.Queue.NS(), Name: job.Payload.Queue.Name()}
	categoryKey := resolveCategoryKey(job.Payload.MessageType, qKey)

	var purged int64

	for {
		select {
		case <-ctx.Done():
			return purged, ctx.Err()
		default:
		}

		canceled, err := isCanceled(ctx, job.ID)
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
			job.Meta = &q.PurgeJobMeta{}
		}
		job.Meta.Purged = purged
		if err := save(ctx, job); err != nil {
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

func (pm *PurgeManager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(workerHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			key := keys.System{}.WorkerHeartbeat(pm.workerID)
			if err := redisClient.Client().Set(ctx, key, "1", workerHeartbeatTTL).Err(); err != nil {
				pm.log.Error("heartbeat failed", "error", err)
			}
		}
	}
}

func (pm *PurgeManager) failJob(ctx context.Context, job *q.PurgeJob, queueParams *q.QueueParams, errMsg string) {
	job.Status = q.PurgeJobFailed
	job.Error = errMsg
	job.CompletedAt = time.Now().UnixMilli()
	fail(ctx, job.ID, job)
	pm.unlockQueue(ctx, queueParams, job.ID, q.PurgeJobFailed, errMsg)
}

func (pm *PurgeManager) recoverStuckJobs(ctx context.Context) {
	jobIDs, err := redisClient.LoadSetMembers(ctx, keys.System{}.ActivePurgeJobs(), "processing purge jobs")
	if err != nil {
		pm.log.Debug("failed to load processing purge jobs", "error", err)
		return
	}

	for _, jobID := range jobIDs {
		if pm.isWorkerAlive(ctx, jobID) {
			continue
		}

		job, err := getJob(ctx, jobID)
		if err != nil {
			continue
		}

		pm.log.Warn("recovering stuck job", "jobID", jobID)

		job.Status = q.PurgeJobPending
		job.Error = "Recovered from worker crash"
		job.UpdatedAt = time.Now().UnixMilli()

		if err := recoverJob(ctx, jobID, job); err != nil {
			pm.log.Error("recover stuck job failed", "jobID", jobID, "error", err)
		}
	}
}

func (pm *PurgeManager) isWorkerAlive(ctx context.Context, jobID string) bool {
	workerID, err := redisClient.Client().Get(ctx, keys.System{}.JobWorker(jobID)).Result()
	if err != nil || workerID == "" {
		return false
	}

	heartbeatKey := keys.System{}.WorkerHeartbeat(workerID)
	exists, err := redisClient.Client().Exists(ctx, heartbeatKey).Result()
	if err != nil {
		return true
	}
	return exists > 0
}

func (pm *PurgeManager) validateFilter(filter q.BrowseFilter) error {
	cfg := config.Get()
	switch filter {
	case q.BrowseAcknowledged:
		if !cfg.MessageAudit.AcknowledgedMessages.Enabled {
			return fmt.Errorf("purge: %w", q.ErrAuditDisabled)
		}
	case q.BrowseDeadLettered:
		if !cfg.MessageAudit.DeadLetteredMessages.Enabled {
			return fmt.Errorf("purge: %w", q.ErrAuditDisabled)
		}
	}
	return nil
}

func (pm *PurgeManager) unlockQueue(ctx context.Context, queueParams *q.QueueParams, jobID string, status q.PurgeJobStatus, description string) {
	var reason q.QueueStateTransitionReason
	switch status {
	case q.PurgeJobCompleted:
		reason = q.QueueStateTransitionReason(q.ReasonPurgeComplete)
	case q.PurgeJobFailed:
		reason = q.QueueStateTransitionReason(q.ReasonPurgeFail)
	case q.PurgeJobCanceled:
		reason = q.QueueStateTransitionReason(q.ReasonPurgeCancel)
	default:
		reason = q.QueueStateTransitionReason(q.ReasonPurgeComplete)
	}

	opts := &q.StateTransitionOptions{
		Description: &description,
	}

	_, err := pm.state.releaseLock(ctx, queueParams, q.LockOwnerPurgeJob, jobID, reason, opts)
	if err != nil {
		pm.log.Error("unlock queue failed", "jobID", jobID, "error", err)
	}
}

func newPurgeJob(jobID string, queueParams *q.QueueParams, filter q.BrowseFilter) *q.PurgeJob {
	return &q.PurgeJob{
		ID: jobID,
		Payload: q.PurgeJobPayload{
			Queue:       queueParams.Clone(),
			MessageType: filter,
		},
		Status:    q.PurgeJobPending,
		BatchSize: defaultPurgeBatchSize,
		DelayMs:   defaultPurgeBatchDelay.Milliseconds(),
		CreatedAt: time.Now().UnixMilli(),
		Meta:      &q.PurgeJobMeta{Purged: 0},
	}
}

func resolveCategoryKey(filter q.BrowseFilter, qKey keys.Queue) string {
	switch filter {
	case q.BrowsePending:
		return qKey.Pending()
	case q.BrowseScheduled:
		return qKey.Scheduled()
	case q.BrowseAcknowledged:
		return qKey.Acknowledged()
	case q.BrowseDeadLettered:
		return qKey.DeadLetter()
	default:
		return ""
	}
}

func acquire(ctx context.Context, conn *rdb.Client) (string, error) {
	return conn.BRPopLPush(
		ctx,
		keys.System{}.PendingPurgeJobs(),
		keys.System{}.ActivePurgeJobs(),
		0,
	).Result()
}
