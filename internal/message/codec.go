/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package message

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/weyoss/go-redis-smq/internal/message/schema"
	"github.com/weyoss/go-redis-smq/pkg/exchange/x"
	"github.com/weyoss/go-redis-smq/pkg/message/msg"
)

// ParamsCodec handles serialization of message.Params to/from JSON.
type ParamsCodec struct{}

// NewParamsCodec creates a new Params codec.
func NewParamsCodec() *ParamsCodec {
	return &ParamsCodec{}
}

// Encode serializes message params to JSON bytes.
func (c *ParamsCodec) Encode(ctx context.Context, params *msg.Params) ([]byte, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode message params: %w", err)
	}
	return data, nil
}

// Decode deserializes JSON bytes to message params.
func (c *ParamsCodec) Decode(ctx context.Context, data []byte) (*msg.Params, error) {
	var params msg.Params
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("decode message params: %w", err)
	}
	return &params, nil
}

// StateCodec handles serialization of MessageState to/from Redis hash.
type StateCodec struct{}

// NewStateCodec creates a new State codec.
func NewStateCodec() *StateCodec {
	return &StateCodec{}
}

// EncodeHash serializes MessageState to a Redis hash map.
func (c *StateCodec) EncodeHash(ctx context.Context, state *msg.MessageState) (map[string]interface{}, error) {
	if state == nil {
		return nil, fmt.Errorf("encode message state: nil")
	}

	t := state.ToTransferable()
	hash := make(map[string]interface{})

	// Core properties
	hash[schema.MessageFieldID.Key()] = t.UUID
	hash[schema.MessageFieldAttempts.Key()] = strconv.Itoa(t.Attempts)
	hash[schema.MessageFieldExpired.Key()] = boolToStr(t.Expired)
	hash[schema.MessageFieldScheduledCronFired.Key()] = boolToStr(t.ScheduledCronFired)
	hash[schema.MessageFieldScheduledRepeatCount.Key()] = strconv.Itoa(t.ScheduledRepeatCount)
	hash[schema.MessageFieldScheduledTimes.Key()] = strconv.Itoa(t.ScheduledTimes)
	hash[schema.MessageFieldRequeueCount.Key()] = strconv.Itoa(t.RequeueCount)
	hash[schema.MessageFieldEffectiveScheduledDelay.Key()] = strconv.FormatInt(t.EffectiveScheduledDelay, 10)

	// Optional timestamps
	setOptionalTS(hash, schema.MessageFieldScheduledAt, t.ScheduledAt)
	setOptionalTS(hash, schema.MessageFieldPublishedAt, t.PublishedAt)
	setOptionalTS(hash, schema.MessageFieldRequeuedAt, t.RequeuedAt)
	setOptionalTS(hash, schema.MessageFieldProcessingStartedAt, t.ProcessingStartedAt)
	setOptionalTS(hash, schema.MessageFieldAcknowledgedAt, t.AcknowledgedAt)
	setOptionalTS(hash, schema.MessageFieldUnacknowledgedAt, t.UnacknowledgedAt)
	setOptionalTS(hash, schema.MessageFieldDeadLetteredAt, t.DeadLetteredAt)
	setOptionalTS(hash, schema.MessageFieldLastRequeuedAt, t.LastRequeuedAt)
	setOptionalTS(hash, schema.MessageFieldLastUnacknowledgedAt, t.LastUnacknowledgedAt)
	setOptionalTS(hash, schema.MessageFieldLastScheduledAt, t.LastScheduledAt)
	setOptionalTS(hash, schema.MessageFieldLastRetriedAttemptAt, t.LastRetriedAttemptAt)
	setOptionalTS(hash, schema.MessageFieldLastProcessedAt, t.LastProcessedAt)

	// Optional parent IDs
	if t.ScheduledMessageParentID != "" {
		hash[schema.MessageFieldScheduledMessageParentID.Key()] = t.ScheduledMessageParentID
	}
	if t.RequeuedMessageParentID != "" {
		hash[schema.MessageFieldRequeuedMessageParentID.Key()] = t.RequeuedMessageParentID
	}

	return hash, nil
}

// DecodeHash deserializes a Redis hash map to MessageState.
func (c *StateCodec) DecodeHash(ctx context.Context, hash map[string]string) (*msg.MessageState, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode message state: empty hash")
	}

	state := msg.NewMessageState()

	// Core properties
	if v, ok := hash[schema.MessageFieldID.Key()]; ok {
		state.SetID(v)
	}
	if v, ok := hash[schema.MessageFieldAttempts.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetAttempts(n)
	}
	if v, ok := hash[schema.MessageFieldExpired.Key()]; ok {
		state.SetExpired(v == "1")
	}
	if v, ok := hash[schema.MessageFieldScheduledCronFired.Key()]; ok {
		state.SetScheduledCronFired(v == "1")
	}
	if v, ok := hash[schema.MessageFieldScheduledRepeatCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledRepeatCount(n)
	}
	if v, ok := hash[schema.MessageFieldScheduledTimes.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledTimes(n)
	}
	if v, ok := hash[schema.MessageFieldRequeueCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetRequeueCount(n)
	}
	if v, ok := hash[schema.MessageFieldEffectiveScheduledDelay.Key()]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		state.SetEffectiveScheduledDelay(n)
	}

	// Optional timestamps
	parseOptionalTS(hash, schema.MessageFieldScheduledAt, state.SetScheduledAt)
	parseOptionalTS(hash, schema.MessageFieldPublishedAt, state.SetPublishedAt)
	parseOptionalTS(hash, schema.MessageFieldRequeuedAt, state.SetRequeuedAt)
	parseOptionalTS(hash, schema.MessageFieldProcessingStartedAt, state.SetProcessingStartedAt)
	parseOptionalTS(hash, schema.MessageFieldAcknowledgedAt, state.SetAcknowledgedAt)
	parseOptionalTS(hash, schema.MessageFieldUnacknowledgedAt, state.SetUnacknowledgedAt)
	parseOptionalTS(hash, schema.MessageFieldDeadLetteredAt, state.SetDeadLetteredAt)
	parseOptionalTS(hash, schema.MessageFieldLastRequeuedAt, state.SetLastRequeuedAt)
	parseOptionalTS(hash, schema.MessageFieldLastUnacknowledgedAt, state.SetLastUnacknowledgedAt)
	parseOptionalTS(hash, schema.MessageFieldLastScheduledAt, state.SetLastScheduledAt)
	parseOptionalTS(hash, schema.MessageFieldLastRetriedAttemptAt, state.SetLastRetriedAttemptAt)
	parseOptionalTS(hash, schema.MessageFieldLastProcessedAt, state.SetLastProcessedAt)

	// Optional parent IDs
	if v, ok := hash[schema.MessageFieldScheduledMessageParentID.Key()]; ok {
		state.SetScheduledMessageParentID(v)
	}
	if v, ok := hash[schema.MessageFieldRequeuedMessageParentID.Key()]; ok {
		state.SetRequeuedMessageParentID(v)
	}

	return state, nil
}

// EnvelopeCodec handles full message envelope serialization.
type EnvelopeCodec struct {
	params *ParamsCodec
	state  *StateCodec
}

// NewEnvelopeCodec creates a new Envelope codec.
func NewEnvelopeCodec() *EnvelopeCodec {
	return &EnvelopeCodec{
		params: NewParamsCodec(),
		state:  NewStateCodec(),
	}
}

// EncodeHash serializes a MessageEnvelope to a Redis hash map.
func (c *EnvelopeCodec) EncodeHash(ctx context.Context, env *Envelope) (map[string]interface{}, error) {
	if env == nil {
		return nil, fmt.Errorf("encode message envelope: nil")
	}

	// Encode the full message params as JSON
	params := env.ToParams()
	paramsJSON, err := c.params.Encode(ctx, params)
	if err != nil {
		return nil, err
	}

	// Start with the JSON payload and status
	hash := map[string]interface{}{
		schema.MessageFieldMessage.Key(): string(paramsJSON),
		schema.MessageFieldStatus.Key():  strconv.Itoa(env.Status().Int()),
	}

	// Merge state fields
	stateHash, err := c.state.EncodeHash(ctx, env.MessageState())
	if err != nil {
		return nil, err
	}
	for k, v := range stateHash {
		hash[k] = v
	}

	return hash, nil
}

// DecodeHash deserializes a Redis hash map to a MessageEnvelope.
func (c *EnvelopeCodec) DecodeHash(ctx context.Context, hash map[string]string) (*Envelope, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode message envelope: empty hash")
	}

	// Decode the JSON payload
	paramsJSON := hash[schema.MessageFieldMessage.Key()]
	params, err := c.params.Decode(ctx, []byte(paramsJSON))
	if err != nil {
		return nil, err
	}

	// Decode status
	statusStr := hash[schema.MessageFieldStatus.Key()]
	status, _ := strconv.Atoi(statusStr)

	// Decode state (all fields except message and status)
	stateHash := make(map[string]string)
	for k, v := range hash {
		if k != schema.MessageFieldMessage.Key() && k != schema.MessageFieldStatus.Key() {
			stateHash[k] = v
		}
	}
	state, err := c.state.DecodeHash(ctx, stateHash)
	if err != nil {
		return nil, err
	}

	// Rebuild the producible message from params
	message := rebuildMessage(params)
	if params.ScheduledDelay != nil {
		state.SetEffectiveScheduledDelay(*params.ScheduledDelay)
	}

	// Build the envelope
	envelope := NewEnvelope(message)
	envelope.SetMessageState(state)
	envelope.SetStatus(msg.MessageStatus(status))
	envelope.SetDestinationQueue(params.DestinationQueue)
	if params.ConsumerGroupID != "" {
		envelope.SetConsumerGroupID(params.ConsumerGroupID)
	}

	return envelope, nil
}

// rebuildMessage creates a ProducibleMessage from deserialized params.
func rebuildMessage(params *msg.Params) *msg.ProducibleMessage {
	msg := msg.New()
	msg.SetBody(params.Body)
	if params.Priority != nil {
		msg.SetPriority(*params.Priority)
	}
	if params.TTL > 0 {
		msg.SetTTL(time.Duration(params.TTL) * time.Millisecond)
	}
	msg.SetRetryThreshold(params.RetryThreshold)
	msg.SetRetryDelay(time.Duration(params.RetryDelay) * time.Millisecond)
	msg.SetConsumeTimeout(time.Duration(params.ConsumeTimeout) * time.Millisecond)
	if params.ScheduledCron != "" {
		msg.SetScheduledCron(params.ScheduledCron)
	}
	if params.ScheduledDelay != nil {
		msg.SetScheduledDelay(time.Duration(*params.ScheduledDelay) * time.Millisecond)
	}
	if params.ScheduledRepeatPeriod != nil {
		msg.SetScheduledRepeatPeriod(time.Duration(*params.ScheduledRepeatPeriod) * time.Millisecond)
	}
	msg.SetScheduledRepeat(params.ScheduledRepeat)
	if params.Exchange != nil {
		switch params.Exchange.Type() {
		case x.TypeDirect:
			msg.SetDirectExchange(params.Exchange)
		case x.TypeFanout:
			msg.SetFanoutExchange(params.Exchange)
		case x.TypeTopic:
			msg.SetTopicExchange(params.Exchange)
		}
	}
	if params.Queue != nil {
		msg.SetQueue(params.Queue)
	}
	return msg
}

// Helper functions

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func setOptionalTS(hash map[string]interface{}, prop schema.MessageField, ts *int64) {
	if ts != nil && *ts > 0 {
		hash[prop.Key()] = strconv.FormatInt(*ts, 10)
	}
}

func parseOptionalTS(hash map[string]string, prop schema.MessageField, setter func(int64)) {
	if v, ok := hash[prop.Key()]; ok {
		ts, err := strconv.ParseInt(v, 10, 64)
		if err == nil && ts > 0 {
			setter(ts)
		}
	}
}
