/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package exchange

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	exSchema "github.com/weyoss/go-redis-smq/internal/exchange/schema"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// FanoutStore handles fanout exchange operations.
// Fanout exchanges broadcast messages to all bound queues, ignoring routing keys.
type FanoutStore struct {
	store      *Store
	validator  *Validator
	codecs     *Codecs
	queueCodec *internalQueue.Codec
}

// NewFanoutStore creates a new fanout exchange store.
func NewFanoutStore(store *Store, validator *Validator, codecs *Codecs) *FanoutStore {
	return &FanoutStore{
		store:      store,
		validator:  validator,
		codecs:     codecs,
		queueCodec: internalQueue.NewCodec(),
	}
}

// Create creates a fanout exchange with the given queue policy.
// Returns ErrTypeMismatch if params.Type() is not TypeFanout.
func (fs *FanoutStore) Create(ctx context.Context, params *pubexchange.Params, policy pubexchange.Policy) error {
	if params.Type() != pubexchange.TypeFanout {
		return pubexchange.ErrTypeMismatch
	}
	return fs.store.Save(ctx, params, policy)
}

// BindQueue binds a queue to a fanout exchange.
// The queue will receive all messages published to this exchange.
// The queue and exchange must be in the same namespace.
func (fs *FanoutStore) BindQueue(
	ctx context.Context,
	queueParams *queue.Params,
	exchangeParams *pubexchange.Params,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return pubexchange.ErrNamespaceMismatch
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := fs.queueCodec.EncodeParams(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode queue: %w", err)
	}

	exchangeStr, err := fs.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.Properties(),
		exKey.FanoutQueues(),
		qKey.ExchangeBindings(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	txf := func(tx *redis.Tx) error {
		_, err := fs.validator.ValidateQueueBinding(ctx, exchangeParams, queueParams)
		if err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.FanoutQueues(), queueStr).Result()
		if err != nil {
			return err
		}
		if isMember {
			return pubexchange.ErrQueueAlreadyBound
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, exKey.Properties(),
				exSchema.ExchangeFieldType.Key(), exchangeParams.Type().Int(),
			)

			pipe.SAdd(ctx, keys.System{}.AllExchanges(), exchangeStr)
			pipe.SAdd(ctx, keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(), exchangeStr)

			pipe.SAdd(ctx, exKey.FanoutQueues(), queueStr)
			pipe.SAdd(ctx, qKey.ExchangeBindings(), exchangeStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

// UnbindQueue removes a queue binding from a fanout exchange.
// The queue and exchange must be in the same namespace.
func (fs *FanoutStore) UnbindQueue(
	ctx context.Context,
	queueParams *queue.Params,
	exchangeParams *pubexchange.Params,
) error {
	if queueParams.NS() != exchangeParams.Namespace() {
		return pubexchange.ErrNamespaceMismatch
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := fs.queueCodec.EncodeParams(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode queue: %w", err)
	}

	exchangeStr, err := fs.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.FanoutQueues(),
		qKey.ExchangeBindings(),
	}

	txf := func(tx *redis.Tx) error {
		if err := fs.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.FanoutQueues(), queueStr).Result()
		if err != nil {
			return err
		}
		if !isMember {
			return pubexchange.ErrQueueNotBound
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SRem(ctx, exKey.FanoutQueues(), queueStr)
			pipe.SRem(ctx, qKey.ExchangeBindings(), exchangeStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

// MatchQueues returns all queues bound to this fanout exchange.
// This is equivalent to BoundQueues for fanout exchanges.
func (fs *FanoutStore) MatchQueues(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
) ([]queue.Params, error) {
	return fs.BoundQueues(ctx, exchangeParams)
}

// BoundQueues returns all queues bound to this fanout exchange.
// It validates that the exchange is a fanout exchange.
func (fs *FanoutStore) BoundQueues(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
) ([]queue.Params, error) {
	if err := fs.store.ValidateType(ctx, exchangeParams, true); err != nil {
		return nil, err
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	members, err := redisClient.LoadSetMembers(ctx,
		exKey.FanoutQueues(),
		fmt.Sprintf("queues for fanout exchange %s/%s", exchangeParams.Namespace(), exchangeParams.Name()))
	if err != nil {
		return nil, err
	}

	return fs.queueCodec.DecodeParamsSlice(ctx, members)
}

// Delete removes a fanout exchange and all its queue bindings.
// Returns error if the exchange has bound queues.
func (fs *FanoutStore) Delete(ctx context.Context, exchangeParams *pubexchange.Params) error {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	exchangeStr, err := fs.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("delete: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.FanoutQueues(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	txf := func(tx *redis.Tx) error {
		if err := fs.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		count, err := tx.SCard(ctx, exKey.FanoutQueues()).Result()
		if err != nil {
			return err
		}
		if count > 0 {
			return pubexchange.ErrHasBoundQueues
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, exKey.Exchange())
			pipe.Del(ctx, exKey.Properties())
			pipe.Del(ctx, exKey.FanoutQueues())
			pipe.SRem(ctx, keys.System{}.AllExchanges(), exchangeStr)
			pipe.SRem(ctx, keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(), exchangeStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}
