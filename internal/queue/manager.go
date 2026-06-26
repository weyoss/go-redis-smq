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
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

type Codecs struct {
	Params codec.SetCodec[*q.QueueParams]
	Props  codec.HashCodec[*q.QueueProps]
}

func DefaultCodecs() *Codecs {
	return &Codecs{
		Params: NewQueueParamsCodec(),
		Props:  NewQueuePropsCodec(),
	}
}

type Manager struct {
	store         *Store
	lookup        *Lookup
	validator     *Validator
	state         *State
	consumerGroup *ConsumerGroupStore
	browse        *Browse
	purge         *PurgeManager
}

func NewManager() *Manager {
	return NewManagerWithCodecs(nil)
}

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
		purge:         NewPurgeManager(state, internalMessage.NewManager().Store()),
	}
}

func (m *Manager) Store() *Store                           { return m.store }
func (m *Manager) Lookup() *Lookup                         { return m.lookup }
func (m *Manager) Validator() *Validator                   { return m.validator }
func (m *Manager) State() *State                           { return m.state }
func (m *Manager) ConsumerGroupStore() *ConsumerGroupStore { return m.consumerGroup }
func (m *Manager) Browse() *Browse                         { return m.browse }
func (m *Manager) Purge() *PurgeManager                    { return m.purge }
