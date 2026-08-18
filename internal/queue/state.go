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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

const maxHistorySize = 100

type State struct{}

func NewState() *State {
	return &State{}
}

func (s *State) FetchCurrent(
	ctx context.Context,
	params *q.QueueParams,
) (*q.StateTransition, error) {
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
			return nil, q.ErrNotFound
		}
		return nil, err
	}

	current := q.StateActive
	if raw != "" {
		if v, parseErr := strconv.Atoi(raw); parseErr == nil {
			current = q.QueueState(v)
		}
	}

	latestJSON, err := client.LIndex(ctx, historyKey, 0).Result()
	if err != nil {
		return newInitialTransition(current), nil
	}

	var t q.StateTransition
	if err := json.Unmarshal([]byte(latestJSON), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *State) FetchHistory(
	ctx context.Context,
	params *q.QueueParams,
) ([]*q.StateTransition, error) {
	qKey := keys.Queue{Namespace: params.NS(), Name: params.Name()}
	client := redisClient.Client()

	rawEntries, err := client.LRange(ctx, qKey.StateHistory(), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	history := make([]*q.StateTransition, 0, len(rawEntries))
	for _, raw := range rawEntries {
		var t q.StateTransition
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			return nil, err
		}
		history = append(history, &t)
	}
	return history, nil
}

// TransitionTo is the public API – reason is taken from opts.
func (s *State) TransitionTo(
	ctx context.Context,
	params *q.QueueParams,
	target q.QueueState,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.transitionTo(ctx, params, target, reason, opts)
}

// AcquireLock is the public API.
func (s *State) AcquireLock(
	ctx context.Context,
	params *q.QueueParams,
	owner q.LockOwner,
	id string,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.acquireLock(ctx, params, owner, id, reason, opts)
}

// ReleaseLock is the public API.
func (s *State) ReleaseLock(
	ctx context.Context,
	params *q.QueueParams,
	owner q.LockOwner,
	id string,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	reason := reasonFromOpts(opts)
	return s.releaseLock(ctx, params, owner, id, reason, opts)
}

// ── Unexported internal methods that accept an explicit reason ──

func (s *State) transitionTo(
	ctx context.Context,
	params *q.QueueParams,
	target q.QueueState,
	reason q.QueueStateTransitionReason,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if !current.To.CanTransitionTo(target) {
		return nil, q.ErrInvalidTransition
	}
	return s.saveState(ctx, params, &current.To, target, reason, opts)
}

func (s *State) acquireLock(
	ctx context.Context,
	params *q.QueueParams,
	owner q.LockOwner,
	id string,
	reason q.QueueStateTransitionReason,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	if id == "" {
		return nil, q.ErrInvalidLock
	}
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if !current.To.CanTransitionTo(q.StateLocked) {
		return nil, q.ErrInvalidTransition
	}
	desc := "Exclusive lock"
	if opts != nil && opts.Description != nil {
		desc = *opts.Description
	}
	lockOpts := &q.StateTransitionOptions{
		Description: &desc,
		LockID:      &id,
		Owner:       &owner,
	}
	if opts != nil && opts.Metadata != nil {
		lockOpts.Metadata = opts.Metadata
	}
	return s.saveState(ctx, params, &current.To, q.StateLocked, reason, lockOpts)
}

func (s *State) releaseLock(
	ctx context.Context,
	params *q.QueueParams,
	owner q.LockOwner,
	id string,
	reason q.QueueStateTransitionReason,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
	if id == "" {
		return nil, q.ErrInvalidLock
	}
	current, err := s.FetchCurrent(ctx, params)
	if err != nil {
		return nil, err
	}
	if current.To != q.StateLocked {
		return nil, q.ErrNotLocked
	}
	if current.Owner == nil || *current.Owner != owner {
		return nil, q.ErrLockOwnerMismatch
	}
	if current.LockID == nil || *current.LockID != id {
		return nil, q.ErrLockIDMismatch
	}
	desc := "Queue unlocked"
	if opts != nil && opts.Description != nil {
		desc = *opts.Description
	}
	unlockOpts := &q.StateTransitionOptions{
		Description: &desc,
		LockID:      &id,
	}
	if opts != nil && opts.Metadata != nil {
		unlockOpts.Metadata = opts.Metadata
	}
	return s.saveState(ctx, params, &current.To, q.StateActive, reason, unlockOpts)
}

func (s *State) saveState(
	ctx context.Context,
	params *q.QueueParams,
	from *q.QueueState,
	to q.QueueState,
	reason q.QueueStateTransitionReason,
	opts *q.StateTransitionOptions,
) (*q.StateTransition, error) {
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
		strconv.Itoa(q.StateActive.Int()),
		maxHistorySize,
		strconv.Itoa(q.StateLocked.Int()),
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

func newInitialTransition(state q.QueueState) *q.StateTransition {
	return &q.StateTransition{
		From:      nil,
		To:        state,
		Reason:    q.QueueStateTransitionReason(q.ReasonSystemInit),
		Timestamp: time.Now().UnixMilli(),
	}
}

func newTransition(
	from *q.QueueState,
	to q.QueueState,
	reason q.QueueStateTransitionReason,
	opts *q.StateTransitionOptions,
) *q.StateTransition {
	t := &q.StateTransition{
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

func reasonFromOpts(opts *q.StateTransitionOptions) q.QueueStateTransitionReason {
	if opts != nil && opts.Reason != nil {
		return q.QueueStateTransitionReason(*opts.Reason)
	}
	return q.QueueStateTransitionReason(q.ReasonManual)
}

func extractLockID(opts *q.StateTransitionOptions) string {
	if opts != nil && opts.LockID != nil {
		return *opts.LockID
	}
	return ""
}

func interpretReply(reply interface{}, t *q.StateTransition) (*q.StateTransition, error) {
	s, err := redisClient.String(reply)
	if err != nil {
		return nil, err
	}
	switch s {
	case "OK":
		return t, nil
	case "QUEUE_NOT_FOUND":
		return nil, q.ErrNotFound
	case "INVALID_STATE_TRANSITION":
		return nil, q.ErrInvalidTransition
	case "INVALID_LOCK":
		return nil, q.ErrInvalidLock
	default:
		return nil, fmt.Errorf("set queue state: unexpected script reply: %s", s)
	}
}
