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
	"github.com/weyoss/go-redis-smq/internal/codec"
	exSchema "github.com/weyoss/go-redis-smq/internal/exchange/schema"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

// FanoutStore handles fanout exchange operations.
// Fanout exchanges broadcast messages to all bound queues, ignoring routing keys.
type FanoutStore struct {
	store      *Store
	validator  *Validator
	codecs     *Codecs
	queueCodec codec.SetCodec[*queue.QueueParams]
}

// NewFanoutStore creates a new fanout exchange store.
func NewFanoutStore(store *Store, validator *Validator, codecs *Codecs) *FanoutStore {
	return &FanoutStore{
		store:      store,
		validator:  validator,
		codecs:     codecs,
		queueCodec: internalQueue.NewQueueParamsCodec(),
	}
}

// BindQueue binds a queue to a fanout exchange.
// The queue will receive all messages published to this exchange.
func (fs *FanoutStore) BindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
) error {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := fs.queueCodec.EncodeSet(ctx, queueParams)
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
			return x.ErrQueueAlreadyBound
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
func (fs *FanoutStore) UnbindQueue(
	ctx context.Context,
	queueParams *queue.QueueParams,
	exchangeParams *x.ExchangeParams,
) error {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := fs.queueCodec.EncodeSet(ctx, queueParams)
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
			return x.ErrQueueNotBound
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

// BoundQueues returns all queues bound to this fanout exchange.
func (fs *FanoutStore) BoundQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]queue.QueueParams, error) {
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

	return internalQueue.DecodeQueueParams(members)
}

// Delete removes a fanout exchange and all its queue bindings.
// Returns error if the exchange has bound queues.
func (fs *FanoutStore) Delete(ctx context.Context, exchangeParams *x.ExchangeParams) error {
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
			return x.ErrHasBoundQueues
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
