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
	"regexp"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/weyoss/go-redis-smq/internal/codec"
	exSchema "github.com/weyoss/go-redis-smq/internal/exchange/schema"
	internalQueue "github.com/weyoss/go-redis-smq/internal/queue"
	redisClient "github.com/weyoss/go-redis-smq/internal/redis"
	"github.com/weyoss/go-redis-smq/internal/redis/keys"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/queue/q"
)

// TopicStore handles topic exchange pattern-based routing operations.
// Topic exchanges route messages using AMQP-style wildcards (* and #).
type TopicStore struct {
	store      *Store
	validator  *Validator
	codecs     *Codecs
	queueCodec codec.SetCodec[*q.QueueParams]
}

// NewTopicStore creates a new topic exchange store.
func NewTopicStore(store *Store, validator *Validator, codecs *Codecs) *TopicStore {
	return &TopicStore{
		store:      store,
		validator:  validator,
		codecs:     codecs,
		queueCodec: internalQueue.NewQueueParamsCodec(),
	}
}

// BindQueue binds a queue to a topic exchange with a binding pattern.
// The pattern must be a valid AMQP-style topic pattern.
func (ts *TopicStore) BindQueue(
	ctx context.Context,
	queueParams *q.QueueParams,
	exchangeParams *x.ExchangeParams,
	pattern string,
) error {
	if !validateTopicPattern(pattern) {
		return x.ErrInvalidPattern
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := ts.queueCodec.EncodeSet(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode queue: %w", err)
	}

	exchangeStr, err := ts.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("bind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.Properties(),
		exKey.BindingPatterns(),
		exKey.PatternQueues(pattern),
		qKey.ExchangeBindings(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	txf := func(tx *redis.Tx) error {
		_, err := ts.validator.ValidateQueueBinding(ctx, exchangeParams, queueParams)
		if err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.PatternQueues(pattern), queueStr).Result()
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

			pipe.SAdd(ctx, exKey.BindingPatterns(), pattern)
			pipe.SAdd(ctx, exKey.PatternQueues(pattern), queueStr)
			pipe.SAdd(ctx, qKey.ExchangeBindings(), exchangeStr)
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

// UnbindQueue removes a queue binding from a topic exchange pattern.
func (ts *TopicStore) UnbindQueue(
	ctx context.Context,
	queueParams *q.QueueParams,
	exchangeParams *x.ExchangeParams,
	pattern string,
) error {
	if !validateTopicPattern(pattern) {
		return x.ErrInvalidPattern
	}

	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}
	qKey := keys.Queue{
		Namespace: queueParams.NS(),
		Name:      queueParams.Name(),
	}

	queueStr, err := ts.queueCodec.EncodeSet(ctx, queueParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode queue: %w", err)
	}

	exchangeStr, err := ts.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("unbind queue: encode exchange: %w", err)
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.BindingPatterns(),
		exKey.PatternQueues(pattern),
		qKey.ExchangeBindings(),
	}

	txf := func(tx *redis.Tx) error {
		if err := ts.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		isMember, err := tx.SIsMember(ctx, exKey.PatternQueues(pattern), queueStr).Result()
		if err != nil {
			return err
		}
		if !isMember {
			return x.ErrQueueNotBound
		}

		allPatterns, err := tx.SMembers(ctx, exKey.BindingPatterns()).Result()
		if err != nil {
			return err
		}

		stillBound := false
		for _, p := range allPatterns {
			if p == pattern {
				continue
			}
			member, err := tx.SIsMember(ctx, exKey.PatternQueues(p), queueStr).Result()
			if err != nil {
				return err
			}
			if member {
				stillBound = true
				break
			}
		}

		count, err := tx.SCard(ctx, exKey.PatternQueues(pattern)).Result()
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SRem(ctx, exKey.PatternQueues(pattern), queueStr)

			if count == 1 {
				pipe.SRem(ctx, exKey.BindingPatterns(), pattern)
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

// MatchQueues returns all queues whose binding patterns match the routing key.
func (ts *TopicStore) MatchQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	routingKey string,
) ([]q.QueueParams, error) {
	patterns, err := ts.Patterns(ctx, exchangeParams)
	if err != nil {
		return nil, err
	}

	var matchedPatterns []string
	for _, pattern := range patterns {
		if matchTopicPattern(routingKey, pattern) {
			matchedPatterns = append(matchedPatterns, pattern)
		}
	}

	if len(matchedPatterns) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool)
	var queues []q.QueueParams

	for _, pattern := range matchedPatterns {
		bound, err := ts.BoundQueues(ctx, exchangeParams, pattern)
		if err != nil {
			return nil, err
		}
		for _, q := range bound {
			key := q.NS() + ":" + q.Name()
			if !seen[key] {
				seen[key] = true
				queues = append(queues, q)
			}
		}
	}

	return queues, nil
}

// Patterns returns all binding patterns registered for this topic exchange.
func (ts *TopicStore) Patterns(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) ([]string, error) {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	return redisClient.LoadSetMembers(ctx, exKey.BindingPatterns(), "binding patterns")
}

// BoundQueues returns all queues bound to a specific pattern.
func (ts *TopicStore) BoundQueues(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
	pattern string,
) ([]q.QueueParams, error) {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	members, err := redisClient.LoadSetMembers(ctx,
		exKey.PatternQueues(pattern),
		fmt.Sprintf("queues for pattern %s", pattern))
	if err != nil {
		return nil, err
	}

	return internalQueue.DecodeQueueParams(members)
}

// Bindings returns all pattern to queue mappings.
func (ts *TopicStore) Bindings(
	ctx context.Context,
	exchangeParams *x.ExchangeParams,
) (map[string][]q.QueueParams, error) {
	patterns, err := ts.Patterns(ctx, exchangeParams)
	if err != nil {
		return nil, err
	}

	bindings := make(map[string][]q.QueueParams, len(patterns))
	for _, pattern := range patterns {
		queues, err := ts.BoundQueues(ctx, exchangeParams, pattern)
		if err != nil {
			return nil, err
		}
		bindings[pattern] = queues
	}
	return bindings, nil
}

// Delete removes a topic exchange and all pattern bindings.
// Returns error if any patterns have bound queues.
func (ts *TopicStore) Delete(ctx context.Context, exchangeParams *x.ExchangeParams) error {
	exKey := keys.Exchange{
		Namespace: exchangeParams.Namespace(),
		Name:      exchangeParams.Name(),
	}

	exchangeStr, err := ts.codecs.Params.EncodeSet(ctx, exchangeParams)
	if err != nil {
		return fmt.Errorf("delete: encode exchange: %w", err)
	}

	allPatterns, err := ts.Patterns(ctx, exchangeParams)
	if err != nil {
		return err
	}

	watchKeys := []string{
		exKey.Exchange(),
		exKey.BindingPatterns(),
		keys.System{}.AllExchanges(),
		keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(),
	}

	for _, p := range allPatterns {
		watchKeys = append(watchKeys, exKey.PatternQueues(p))
	}

	txf := func(tx *redis.Tx) error {
		if err := ts.store.ValidateType(ctx, exchangeParams, true); err != nil {
			return err
		}

		for _, p := range allPatterns {
			count, err := tx.SCard(ctx, exKey.PatternQueues(p)).Result()
			if err != nil {
				return err
			}
			if count > 0 {
				return x.ErrHasBoundQueues
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, exKey.Exchange())
			pipe.Del(ctx, exKey.Properties())
			pipe.Del(ctx, exKey.BindingPatterns())
			pipe.SRem(ctx, keys.System{}.AllExchanges(), exchangeStr)
			pipe.SRem(ctx, keys.Namespace{Name: exchangeParams.Namespace()}.Exchanges(), exchangeStr)

			for _, p := range allPatterns {
				pipe.Del(ctx, exKey.PatternQueues(p))
			}
			return nil
		})
		return err
	}

	return redisClient.WithTransaction(ctx, watchKeys, 5, txf)
}

// Topic pattern matching logic

var literalTokenRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*$`)

func validateTopicPattern(pattern string) bool {
	if len(pattern) == 0 {
		return false
	}

	tokens := strings.Split(pattern, ".")
	for _, token := range tokens {
		if len(token) == 0 {
			return false
		}
		if token == "*" || token == "#" {
			continue
		}
		if !literalTokenRE.MatchString(token) {
			return false
		}
	}
	return true
}

func matchTopicPattern(routingKey, pattern string) bool {
	tokens := strings.Split(pattern, ".")
	var reParts []string
	prevWasHash := false

	for i, token := range tokens {
		if token == "#" {
			if i == 0 {
				reParts = append(reParts, `(?:[^.]+(?:\.[^.]+)*)?`)
			} else {
				reParts = append(reParts, `(?:\.[^.]+)*`)
			}
			prevWasHash = true
			continue
		}

		if i > 0 {
			if prevWasHash {
				reParts = append(reParts, `(?:\.)?`)
			} else {
				reParts = append(reParts, `\.`)
			}
		}
		prevWasHash = false

		if token == "*" {
			reParts = append(reParts, `[^.]+`)
		} else {
			reParts = append(reParts, regexp.QuoteMeta(token))
		}
	}

	re := regexp.MustCompile(`^` + strings.Join(reParts, "") + `$`)
	return re.MatchString(routingKey)
}
