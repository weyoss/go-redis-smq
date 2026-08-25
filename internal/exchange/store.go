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
	"strconv"

	"github.com/redis/go-redis/v9"
	exSchema "github.com/weyoss/go-redis-smq/internal/exchange/schema"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	pubexchange "github.com/weyoss/go-redis-smq/pkg/exchange"
)

type Store struct {
	codecs *Codecs
}

func NewStore(codecs *Codecs) *Store {
	if codecs == nil {
		codecs = DefaultCodecs()
	}
	return &Store{codecs: codecs}
}

func (s *Store) Save(ctx context.Context, params *pubexchange.ExchangeParams, policy pubexchange.ExchangePolicy) error {
	key := keys.Exchange{
		Namespace: params.Namespace(),
		Name:      params.Name(),
	}

	paramsStr, err := s.codecs.Params.EncodeSet(ctx, params)
	if err != nil {
		return fmt.Errorf("save exchange: %w", err)
	}

	props := &pubexchange.ExchangeProps{
		Type:   params.Type(),
		Policy: policy,
	}
	propsHash, err := s.codecs.Props.EncodeHash(ctx, props)
	if err != nil {
		return fmt.Errorf("save exchange: %w", err)
	}

	watchKeys := []string{
		key.Properties(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: params.Namespace()}.Exchanges(),
	}

	txf := func(tx *redis.Tx) error {
		exists, err := tx.HExists(ctx, key.Properties(),
			exSchema.ExchangeFieldType.Key()).Result()
		if err != nil {
			return err
		}
		if exists {
			return pubexchange.ErrAlreadyExists
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, key.Properties(), propsHash)
			pipe.SAdd(ctx, keys.System{}.AllNamespaces(), params.Namespace())
			pipe.SAdd(ctx, keys.System{}.AllExchanges(), paramsStr)
			pipe.SAdd(ctx, keys.Namespace{Name: params.Namespace()}.Exchanges(), paramsStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 3, txf)
}

func (s *Store) Load(ctx context.Context, params *pubexchange.ExchangeParams) (*pubexchange.ExchangeProps, error) {
	key := keys.Exchange{
		Namespace: params.Namespace(),
		Name:      params.Name(),
	}.Properties()

	hash, err := redisClient.LoadHash(ctx, key, "exchange properties")
	if err != nil {
		return nil, pubexchange.ErrNotFound
	}

	props, err := s.codecs.Props.DecodeHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("load exchange: %w", err)
	}

	return props, nil
}

func (s *Store) Exists(ctx context.Context, params *pubexchange.ExchangeParams) (bool, error) {
	key := keys.Exchange{
		Namespace: params.Namespace(),
		Name:      params.Name(),
	}.Properties()

	count, err := redisClient.Client().Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("check exchange exists: %w", err)
	}
	return count > 0, nil
}

func (s *Store) ValidateType(ctx context.Context, params *pubexchange.ExchangeParams, required bool) error {
	key := keys.Exchange{
		Namespace: params.Namespace(),
		Name:      params.Name(),
	}.Properties()

	storedType, err := redisClient.LoadHashField(ctx, key,
		exSchema.ExchangeFieldType.Key(), "exchange type")
	if err != nil {
		if required {
			return pubexchange.ErrNotFound
		}
		return nil
	}

	typeValue, err := strconv.Atoi(storedType)
	if err != nil {
		return fmt.Errorf("parse exchange type: %w", err)
	}

	actualType := pubexchange.ExchangeType(typeValue)
	if actualType != params.Type() {
		return pubexchange.ErrTypeMismatch
	}
	return nil
}

// Delete removes an exchange and all its associated data structures.
// It first checks that no queues are bound to the exchange; if any exist,
// it returns pubexchange.ErrHasBoundQueues.
func (s *Store) Delete(ctx context.Context, params *pubexchange.ExchangeParams) error {
	key := keys.Exchange{
		Namespace: params.Namespace(),
		Name:      params.Name(),
	}

	// Ensure the exchange exists.
	exists, err := s.Exists(ctx, params)
	if err != nil {
		return err
	}
	if !exists {
		return pubexchange.ErrNotFound
	}

	// Check for bound queues depending on exchange type.
	switch params.Type() {
	case pubexchange.TypeDirect:
		routingKeys, err := redisClient.LoadSetMembers(ctx, key.RoutingKeys(), "routing keys")
		if err != nil {
			return err
		}
		for _, rk := range routingKeys {
			count, err := redisClient.Client().SCard(ctx, key.RoutingKeyQueues(rk)).Result()
			if err != nil {
				return err
			}
			if count > 0 {
				return pubexchange.ErrHasBoundQueues
			}
		}
	case pubexchange.TypeTopic:
		patterns, err := redisClient.LoadSetMembers(ctx, key.BindingPatterns(), "binding patterns")
		if err != nil {
			return err
		}
		for _, p := range patterns {
			count, err := redisClient.Client().SCard(ctx, key.PatternQueues(p)).Result()
			if err != nil {
				return err
			}
			if count > 0 {
				return pubexchange.ErrHasBoundQueues
			}
		}
	case pubexchange.TypeFanout:
		count, err := redisClient.Client().SCard(ctx, key.FanoutQueues()).Result()
		if err != nil {
			return err
		}
		if count > 0 {
			return pubexchange.ErrHasBoundQueues
		}
	}

	// No bound queues; proceed with deletion.
	paramsStr, err := s.codecs.Params.EncodeSet(ctx, params)
	if err != nil {
		return fmt.Errorf("delete exchange: %w", err)
	}

	pipe := redisClient.Client().Pipeline()

	pipe.Del(ctx, key.Exchange())
	pipe.Del(ctx, key.Properties())

	pipe.SRem(ctx, keys.System{}.AllExchanges(), paramsStr)
	pipe.SRem(ctx, keys.Namespace{Name: params.Namespace()}.Exchanges(), paramsStr)

	switch params.Type() {
	case pubexchange.TypeDirect:
		pipe.Del(ctx, key.RoutingKeys())
	case pubexchange.TypeTopic:
		pipe.Del(ctx, key.BindingPatterns())
	case pubexchange.TypeFanout:
		pipe.Del(ctx, key.FanoutQueues())
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("delete exchange: %w", err)
	}
	return nil
}
