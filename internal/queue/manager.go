/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package queue provides internal implementations for RedisSMQ queue
// operations. This file defines the Manager type, which serves both as the
// internal orchestrator and as the concrete implementation of the public
// queue.Manager interface.
package queue

import (
	"context"

	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Manager is the internal orchestrator for queue operations.
// It holds all subcomponents and also implements the public queue.Manager
// interface by delegating to those components.
type Manager struct {
	store         *Store
	lookup        *Lookup
	validator     *Validator
	state         *State
	consumerGroup *ConsumerGroupStore
	browse        *Browse
	purge         *PurgeManager
}

// NewManager creates a new internal queue manager with default codecs.
// It returns the concrete *Manager for internal use, exposing access to
// subcomponents (Store, Lookup, etc.).
func NewManager() *Manager {
	return NewManagerWithCodecs(nil)
}

// NewManagerWithCodecs creates a new internal queue manager with custom codecs.
func NewManagerWithCodecs(codecs *Codecs) *Manager {
	if codecs == nil {
		codecs = DefaultCodecs()
	}

	store := NewStore(codecs)
	state := NewState()

	return &Manager{
		store:         store,
		lookup:        NewLookup(codecs),
		validator:     NewValidator(store),
		state:         state,
		consumerGroup: NewConsumerGroupStore(),
		browse:        NewBrowse(store),
		purge:         NewPurgeManager(state, internalMessage.NewStore(nil, nil)),
	}
}

// NewPublicManager returns a new queue manager that satisfies the public
// queue.Manager interface. It is used by the root redissmq package to expose
// the public API.
func NewPublicManager() publicqueue.Manager {
	return NewManager()
}

// Store returns the internal store component.
func (m *Manager) Store() *Store { return m.store }

// Lookup returns the internal lookup component.
func (m *Manager) Lookup() *Lookup { return m.lookup }

// Validator returns the internal validator component.
func (m *Manager) Validator() *Validator { return m.validator }

// State returns the internal state management component.
func (m *Manager) State() *State { return m.state }

// ConsumerGroupStore returns the internal consumer group store.
func (m *Manager) ConsumerGroupStore() *ConsumerGroupStore { return m.consumerGroup }

// Browse returns the internal browse component.
func (m *Manager) Browse() *Browse { return m.browse }

// Purge returns the internal purge manager.
func (m *Manager) Purge() *PurgeManager { return m.purge }

// Create implements publicqueue.Manager.
func (m *Manager) Create(ctx context.Context, params *publicqueue.Params, queueType publicqueue.Type, deliveryModel publicqueue.DeliveryModel) error {
	return m.store.Save(ctx, params, queueType, deliveryModel)
}

// CreateWithRateLimit implements publicqueue.Manager.
func (m *Manager) CreateWithRateLimit(ctx context.Context, params *publicqueue.Params, queueType publicqueue.Type, deliveryModel publicqueue.DeliveryModel, rl *publicqueue.RateLimitParams) error {
	return m.store.SaveWithRateLimit(ctx, params, queueType, deliveryModel, rl)
}

// Properties implements publicqueue.Manager.
func (m *Manager) Properties(ctx context.Context, params *publicqueue.Params) (*publicqueue.Props, error) {
	return m.store.Load(ctx, params)
}

// Exists implements publicqueue.Manager.
func (m *Manager) Exists(ctx context.Context, params *publicqueue.Params) (bool, error) {
	return m.store.Exists(ctx, params)
}

// Delete implements publicqueue.Manager.
func (m *Manager) Delete(ctx context.Context, params *publicqueue.Params) error {
	return m.store.Delete(ctx, params)
}

// ListAll implements publicqueue.Manager.
func (m *Manager) ListAll(ctx context.Context) ([]publicqueue.Params, error) {
	return m.lookup.All(ctx)
}

// ListByNamespace implements publicqueue.Manager.
func (m *Manager) ListByNamespace(ctx context.Context, namespace string) ([]publicqueue.Params, error) {
	return m.lookup.ByNamespace(ctx, namespace)
}

// BrowseMessages implements publicqueue.Manager.
func (m *Manager) BrowseMessages(ctx context.Context, queueParams *publicqueue.Params, params *publicqueue.BrowseParams) (*publicqueue.BrowseResult, error) {
	return m.browse.BrowseMessages(ctx, queueParams, params)
}

// PurgeQueue implements publicqueue.Manager.
func (m *Manager) PurgeQueue(ctx context.Context, queueParams *publicqueue.Params, filter publicqueue.BrowseFilter) (string, error) {
	return m.purge.Enqueue(ctx, queueParams, filter)
}

// GetPurgeJob implements publicqueue.Manager.
func (m *Manager) GetPurgeJob(ctx context.Context, jobID string) (*publicqueue.PurgeJob, error) {
	return m.purge.Get(ctx, jobID)
}

// CancelPurgeJob implements publicqueue.Manager.
func (m *Manager) CancelPurgeJob(ctx context.Context, queueParams *publicqueue.Params, jobID string) error {
	return m.purge.Cancel(ctx, queueParams, jobID)
}

// SetRateLimit implements publicqueue.Manager.
func (m *Manager) SetRateLimit(ctx context.Context, params *publicqueue.Params, rl *publicqueue.RateLimitParams) error {
	return m.store.SetRateLimit(ctx, params, rl)
}

// ClearRateLimit implements publicqueue.Manager.
func (m *Manager) ClearRateLimit(ctx context.Context, params *publicqueue.Params) error {
	return m.store.ClearRateLimit(ctx, params)
}

// RateLimit implements publicqueue.Manager.
func (m *Manager) RateLimit(ctx context.Context, params *publicqueue.Params) (*publicqueue.RateLimitParams, error) {
	return m.store.GetRateLimit(ctx, params)
}

// MustExist implements publicqueue.Manager.
func (m *Manager) MustExist(ctx context.Context, params *publicqueue.Params) error {
	return m.validator.Exists(ctx, params)
}

// MustBeOperational implements publicqueue.Manager.
func (m *Manager) MustBeOperational(ctx context.Context, params *publicqueue.Params) error {
	return m.validator.IsOperational(ctx, params)
}

// CanEnqueue implements publicqueue.Manager.
func (m *Manager) CanEnqueue(ctx context.Context, params *publicqueue.Params) error {
	return m.validator.CanEnqueue(ctx, params)
}

// CanDequeue implements publicqueue.Manager.
func (m *Manager) CanDequeue(ctx context.Context, params *publicqueue.Params) error {
	return m.validator.CanDequeue(ctx, params)
}
