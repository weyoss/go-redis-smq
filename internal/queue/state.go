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
	"strconv"
	"time"

	queueEvents "github.com/weyoss/go-redis-smq/internal/queue/events"
	"github.com/weyoss/go-redis-smq/internal/queue/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/internal/redis/scripts"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

const maxHistorySize = 100

// State is the internal implementation of queue state management.
type State struct{}

// NewState creates a new internal state manager.
func NewState() *State {
	return &State{}
}

// FetchCurrent returns the latest state transition for a queue.
func (s *State) FetchCurrent(
	ctx context.Context,
	params *publicqueue.QueueParams,
) (*publicqueue.StateTransition, error) {
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	propsKey := qKey.Properties()
	historyKey := qKey.StateHistory()
	client := redisClient.Client()

	raw, err := client.HGet(ctx, propsKey, schema.QueueFieldOperationalState.Key()).Result()
	if err != nil {
		exists, existsErr := client.Exists(ctx, propsKey).Result()
		if existsErr != nil {
			return nil, existsErr
		}
		if exists == 0 {
			return nil, publicqueue.ErrNotFound
		}
		return nil, err
	}

	current := publicqueue.StateActive
	if raw != "" {
		if v, parseErr := strconv.Atoi(raw); parseErr == nil {
			current = publicqueue.QueueState(v)
		}
	}

	latestJSON, err := client.LIndex(ctx, historyKey, 0).Result()
	if err != nil {
		return newInitialTransition(current), nil
	}

	var t publicqueue.StateTransition
	if err := json.Unmarshal([]byte(latestJSON), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// FetchHistory returns the full state transition history for a queue.
func (s *State) FetchHistory(
	ctx context.Context,
	params *publicqueue.QueueParams,
) ([]*publicqueue.StateTransition, error) {
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	client := redisClient.Client()

	rawEntries, err := client.LRange(ctx, qKey.StateHistory(), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	history := make([]*publicqueue.StateTransition, 0, len(rawEntries))
	for _, raw := range rawEntries {
		var t publicqueue.StateTransition
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			return nil, err
		}
		history = append(history, &t)
	}
	return history, nil
}

// TransitionTo is the public state transition method. It derives the reason
// from the given options and calls the internal transitionTo.
func (s *State) TransitionTo(
	ctx context.Context,
	params *publicqueue.QueueParams,
	target publicqueue.QueueState,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.transitionTo(ctx, params, target, reason, opts)
}

// AcquireLock is the public lock acquisition method. It derives the reason
// and calls the internal acquireLock.
func (s *State) AcquireLock(
	ctx context.Context,
	params *publicqueue.QueueParams,
	owner publicqueue.LockOwner,
	id string,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.acquireLock(ctx, params, owner, id, reason, opts)
}

// ReleaseLock is the public lock release method. It derives the reason and
// calls the internal releaseLock.
func (s *State) ReleaseLock(
	ctx context.Context,
	params *publicqueue.QueueParams,
	owner publicqueue.LockOwner,
	id string,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.releaseLock(ctx, params, owner, id, reason, opts)
}

// transitionTo performs a state transition with an explicit reason.
func (s *State) transitionTo(
	ctx context.Context,
	params *publicqueue.QueueParams,
	target publicqueue.QueueState,
	reason publicqueue.QueueStateTransitionReason,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if !current.To.CanTransitionTo(target) {
		return nil, publicqueue.ErrInvalidTransition
	}
	return s.saveState(ctx, params, &current.To, target, reason, opts)
}

// acquireLock acquires a lock with an explicit reason. Used by purge manager.
func (s *State) acquireLock(
	ctx context.Context,
	params *publicqueue.QueueParams,
	owner publicqueue.LockOwner,
	id string,
	reason publicqueue.QueueStateTransitionReason,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	if id == "" {
		return nil, publicqueue.ErrInvalidLock
	}
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if !current.To.CanTransitionTo(publicqueue.StateLocked) {
		return nil, publicqueue.ErrInvalidTransition
	}
	desc := "Exclusive lock"
	if opts != nil && opts.Description != nil {
		desc = *opts.Description
	}
	lockOpts := &publicqueue.StateTransitionOptions{
		Description: &desc,
		LockID:      &id,
		Owner:       &owner,
	}
	if opts != nil && opts.Metadata != nil {
		lockOpts.Metadata = opts.Metadata
	}
	return s.saveState(ctx, params, &current.To, publicqueue.StateLocked, reason, lockOpts)
}

// releaseLock releases a lock with an explicit reason. Used by purge manager.
func (s *State) releaseLock(
	ctx context.Context,
	params *publicqueue.QueueParams,
	owner publicqueue.LockOwner,
	id string,
	reason publicqueue.QueueStateTransitionReason,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	if id == "" {
		return nil, publicqueue.ErrInvalidLock
	}
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if current.To != publicqueue.StateLocked {
		return nil, publicqueue.ErrNotLocked
	}
	if current.Owner == nil || *current.Owner != owner {
		return nil, publicqueue.ErrLockOwnerMismatch
	}
	if current.LockID == nil || *current.LockID != id {
		return nil, publicqueue.ErrLockIDMismatch
	}
	desc := "Queue unlocked"
	if opts != nil && opts.Description != nil {
		desc = *opts.Description
	}
	unlockOpts := &publicqueue.StateTransitionOptions{
		Description: &desc,
		LockID:      &id,
	}
	if opts != nil && opts.Metadata != nil {
		unlockOpts.Metadata = opts.Metadata
	}
	return s.saveState(ctx, params, &current.To, publicqueue.StateActive, reason, unlockOpts)
}

// saveState persists the new state transition and publishes an event.
func (s *State) saveState(
	ctx context.Context,
	params *publicqueue.QueueParams,
	from *publicqueue.QueueState,
	to publicqueue.QueueState,
	reason publicqueue.QueueStateTransitionReason,
	opts *publicqueue.StateTransitionOptions,
) (*publicqueue.StateTransition, error) {
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}

	t := newTransition(from, to, reason, opts)

	tJSON, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	expectedPrev := ""
	if from != nil {
		expectedPrev = strconv.Itoa(from.Int())
	}

	luaKeys := []string{qKey.Properties(), qKey.StateHistory()}
	argv := []interface{}{
		schema.QueueFieldOperationalState.Key(),
		strconv.Itoa(to.Int()),
		string(tJSON),
		expectedPrev,
		strconv.Itoa(publicqueue.StateActive.Int()),
		maxHistorySize,
		strconv.Itoa(publicqueue.StateLocked.Int()),
		extractLockID(opts),
		schema.QueueFieldLastStateChangeAt.Key(),
		strconv.FormatInt(t.Timestamp, 10),
		schema.QueueFieldLockID.Key(),
	}

	reply, err := redisClient.Eval(ctx, scripts.SetQueueState, luaKeys, argv...)
	if err != nil {
		return nil, err
	}

	result, err := interpretReply(reply, t)
	if err != nil {
		return nil, err
	}

	queueEvents.PublishStateChanged(ctx, *params, *t)

	return result, nil
}

func newInitialTransition(state publicqueue.QueueState) *publicqueue.StateTransition {
	return &publicqueue.StateTransition{
		From:      nil,
		To:        state,
		Reason:    publicqueue.QueueStateTransitionReason(publicqueue.ReasonSystemInit),
		Timestamp: time.Now().UnixMilli(),
	}
}

func newTransition(
	from *publicqueue.QueueState,
	to publicqueue.QueueState,
	reason publicqueue.QueueStateTransitionReason,
	opts *publicqueue.StateTransitionOptions,
) *publicqueue.StateTransition {
	t := &publicqueue.StateTransition{
		From:      from,
		To:        to,
		Reason:    reason,
		Timestamp: time.Now().UnixMilli(),
		Metadata:  make(map[string]interface{}),
	}

	if opts == nil {
		return t
	}
	if opts.Description != nil {
		t.Description = *opts.Description
	}
	if opts.LockID != nil {
		t.LockID = opts.LockID
	}
	if opts.Owner != nil {
		t.Owner = opts.Owner
	}
	if opts.Metadata != nil {
		t.Metadata = opts.Metadata
	}
	return t
}

func reasonFromOpts(opts *publicqueue.StateTransitionOptions) publicqueue.QueueStateTransitionReason {
	if opts != nil && opts.Reason != nil {
		return publicqueue.QueueStateTransitionReason(*opts.Reason)
	}
	return publicqueue.QueueStateTransitionReason(publicqueue.ReasonManual)
}

func extractLockID(opts *publicqueue.StateTransitionOptions) string {
	if opts != nil && opts.LockID != nil {
		return *opts.LockID
	}
	return ""
}

func interpretReply(reply interface{}, t *publicqueue.StateTransition) (*publicqueue.StateTransition, error) {
	s, err := redisClient.String(reply)
	if err != nil {
		return nil, err
	}
	switch s {
	case "OK":
		return t, nil
	case "QUEUE_NOT_FOUND":
		return nil, publicqueue.ErrNotFound
	case "INVALID_STATE_TRANSITION":
		return nil, publicqueue.ErrInvalidTransition
	case "INVALID_LOCK":
		return nil, publicqueue.ErrInvalidLock
	default:
		return nil, fmt.Errorf("set queue state: unexpected script reply: %s", s)
	}
}
