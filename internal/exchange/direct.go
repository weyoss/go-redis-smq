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
	internalqueue "github.com/weyoss/go-redis-smq/internal/queue"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
	"github.com/weyoss/go-redis-smq/pkg/queue"
)

type DirectStore struct {
	store      *Store
	validator  *Validator
	codec      *Codec
	queueCodec *internalqueue.Codec
}

func NewDirectStore(store *Store, validator *Validator, codec *Codec) *DirectStore {
	return &DirectStore{
		store:      store,
		validator:  validator,
		codec:      codec,
		queueCodec: internalqueue.NewCodec(),
	}
}

// Create creates a direct exchange with the given queue policy.
func (ds *DirectStore) Create(ctx context.Context, params *pubexchange.Params, policy pubexchange.Policy) error {
	if params.Type() != pubexchange.TypeDirect {
		return pubexchange.ErrTypeMismatch
	}
	return ds.store.Save(ctx, params, policy)
}

func (ds *DirectStore) BindQueue(
	ctx context.Context,
	queueParams *queue.Params,
	exchangeParams *pubexchange.Params,
	routingKey string,
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

	queueStr, err := ds.queueCodec.EncodeParams(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode queue: %w", err)
	}

	exchangeStr, err := ds.codec.EncodeParams(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.Properties(),
		exKey.RoutingKeys(),
		exKey.RoutingKeyQueues(routingKey),
		qKey.ExchangeBindings(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	txf := func(tx *redis.Tx) error {
		_, err := ds.validator.ValidateQueueBinding(ctx, exchangeParams, queueParams)
		if err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.RoutingKeyQueues(routingKey), queueStr).Result()
		if err != nil {
			return err
		}
		if isMember {
			return pubexchange.ErrQueueAlreadyBound
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, exKey.Properties(),
				exSchema.Type.Key(), exchangeParams.Type().Int(),
			)

			pipe.SAdd(ctx, keys.System{}.AllExchanges(), exchangeStr)
			pipe.SAdd(ctx, keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(), exchangeStr)

			pipe.SAdd(ctx, exKey.RoutingKeys(), routingKey)
			pipe.SAdd(ctx, exKey.RoutingKeyQueues(routingKey), queueStr)
			pipe.SAdd(ctx, qKey.ExchangeBindings(), exchangeStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

func (ds *DirectStore) UnbindQueue(
	ctx context.Context,
	queueParams *queue.Params,
	exchangeParams *pubexchange.Params,
	routingKey string,
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

	queueStr, err := ds.queueCodec.EncodeParams(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode queue: %w", err)
	}

	exchangeStr, err := ds.codec.EncodeParams(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.RoutingKeys(),
		exKey.RoutingKeyQueues(routingKey),
		qKey.ExchangeBindings(),
	}

	txf := func(tx *redis.Tx) error {
		if err := ds.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.RoutingKeyQueues(routingKey), queueStr).Result()
		if err != nil {
			return err
		}
		if !isMember {
			return pubexchange.ErrQueueNotBound
		}

		allKeys, err := tx.SMembers(ctx, exKey.RoutingKeys()).Result()
		if err != nil {
			return err
		}

		stillBound := false
		for _, rk := range allKeys {
			if rk == routingKey {
				continue
			}
			member, err := tx.SIsMember(ctx, exKey.RoutingKeyQueues(rk), queueStr).Result()
			if err != nil {
				return err
			}
			if member {
				stillBound = true
				break
			}
		}

		count, err := tx.SCard(ctx, exKey.RoutingKeyQueues(routingKey)).Result()
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SRem(ctx, exKey.RoutingKeyQueues(routingKey), queueStr)

			if count == 1 {
				pipe.SRem(ctx, exKey.RoutingKeys(), routingKey)
			}

			if !stillBound {
				pipe.SRem(ctx, qKey.ExchangeBindings(), exchangeStr)
			}
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

func (ds *DirectStore) MatchQueues(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
	routingKey string,
) ([]queue.Params, error) {
	if err := ds.store.ValidateType(ctx, exchangeParams, true); err != nil {
		return nil, err
	}

	return ds.BoundQueues(ctx, exchangeParams, routingKey)
}

func (ds *DirectStore) RoutingKeys(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
) ([]string, error) {
	if err := ds.store.ValidateType(ctx, exchangeParams, true); err != nil {
		return nil, err
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	return redisClient.LoadSetMembers(ctx, exKey.RoutingKeys(), "routing keys")
}

func (ds *DirectStore) BoundQueues(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
	routingKey string,
) ([]queue.Params, error) {
	if err := ds.store.ValidateType(ctx, exchangeParams, true); err != nil {
		return nil, err
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	members, err := redisClient.LoadSetMembers(ctx,
		exKey.RoutingKeyQueues(routingKey),
		fmt.Sprintf("queues for routing key %s", routingKey))
	if err != nil {
		return nil, err
	}

	return ds.queueCodec.DecodeParamsSlice(ctx, members)
}

func (ds *DirectStore) Bindings(
	ctx context.Context,
	exchangeParams *pubexchange.Params,
) (map[string][]queue.Params, error) {
	routingKeys, err := ds.RoutingKeys(ctx, exchangeParams)
	if err != nil {
		return nil, err
	}

	bindings := make(map[string][]queue.Params, len(routingKeys))
	for _, rk := range routingKeys {
		queues, err := ds.BoundQueues(ctx, exchangeParams, rk)
		if err != nil {
			return nil, err
		}
		bindings[rk] = queues
	}
	return bindings, nil
}

func (ds *DirectStore) Delete(ctx context.Context, exchangeParams *pubexchange.Params) error {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	exchangeStr, err := ds.codec.EncodeParams(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("delete: encode exchange: %w", err)
	}

	allKeys, err := ds.RoutingKeys(ctx, exchangeParams)
	if err != nil {
		return err
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.RoutingKeys(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	for _, rk := range allKeys {
		watchKeys = append(watchKeys, exKey.RoutingKeyQueues(rk))
	}

	txf := func(tx *redis.Tx) error {
		if err := ds.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		for _, rk := range allKeys {
			count, err := tx.SCard(ctx, exKey.RoutingKeyQueues(rk)).Result()
			if err != nil {
				return err
			}
			if count > 0 {
				return pubexchange.ErrHasBoundQueues
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, exKey.Exchange())
			pipe.Del(ctx, exKey.Properties())
			pipe.Del(ctx, exKey.RoutingKeys())
			pipe.SRem(ctx, keys.System{}.AllExchanges(), exchangeStr)
			pipe.SRem(ctx, keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(), exchangeStr)

			for _, rk := range allKeys {
				pipe.Del(ctx, exKey.RoutingKeyQueues(rk))
			}
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}
