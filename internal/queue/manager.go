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
	"github.com/weyoss/go-redis-smq/internal/codec"
	internalMessage "github.com/weyoss/go-redis-smq/internal/message"
	publicqueue "github.com/weyoss/go-redis-smq/pkg/queue"
)

// Codecs holds codec instances for queue serialization.
type Codecs struct {
	Params codec.SetCodec[*publicqueue.Params]
	Props  codec.HashCodec[*publicqueue.Props]
}

// DefaultCodecs returns the standard TypeScript-compatible codecs.
func DefaultCodecs() *Codecs {
	return &Codecs{
		Params: NewQueueParamsCodec(),
		Props:  NewQueuePropsCodec(),
	}
}

// Manager provides the internal orchestration layer for queue operations.
//
// It holds references to all internal queue subcomponents (store, lookup,
// validator, state, consumer groups, browsing, purge) and is used by the
// concrete public interface implementations in queue_manager.go,
// state_manager.go, and consumer_group_manager.go.
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
func NewManager() *Manager {
	return NewManagerWithCodecs(nil)
}

// NewManagerWithCodecs creates a new internal queue manager with custom codecs.
// Passing nil will use the default codecs.
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
