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

	"github.com/weyoss/go-redis-smq/pkg/exchange"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
)

// ParamsCodec handles serialization of message.Params to/from JSON.
type ParamsCodec struct{}

// NewParamsCodec creates a new Params codec.
func NewParamsCodec() *ParamsCodec {
	return &ParamsCodec{}
}

// Encode serializes message params to JSON bytes.
func (c *ParamsCodec) Encode(_ context.Context, params *publicmessage.Params) ([]byte, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode message params: %w", err)
	}
	return data, nil
}

// Decode deserializes JSON bytes to message params.
func (c *ParamsCodec) Decode(_ context.Context, data []byte) (*publicmessage.Params, error) {
	var params publicmessage.Params
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("decode message params: %w", err)
	}
	return &params, nil
}

// StateCodec handles serialization of State to/from Redis hash.
type StateCodec struct{}

// NewStateCodec creates a new State codec.
func NewStateCodec() *StateCodec {
	return &StateCodec{}
}

// EncodeHash serializes State to a Redis hash map.
func (c *StateCodec) EncodeHash(_ context.Context, state *publicmessage.State) (map[string]interface{}, error) {
	if state == nil {
		return nil, fmt.Errorf("encode message state: nil")
	}

	t := state.ToTransferable()
	hash := make(map[string]interface{})

	// Core properties
	hash[MessageFieldID.Key()] = t.UUID
	hash[MessageFieldAttempts.Key()] = strconv.Itoa(t.Attempts)
	hash[MessageFieldExpired.Key()] = boolToStr(t.Expired)
	hash[MessageFieldScheduledCronFired.Key()] = boolToStr(t.ScheduledCronFired)
	hash[MessageFieldScheduledRepeatCount.Key()] = strconv.Itoa(t.ScheduledRepeatCount)
	hash[MessageFieldScheduledTimes.Key()] = strconv.Itoa(t.ScheduledTimes)
	hash[MessageFieldRequeueCount.Key()] = strconv.Itoa(t.RequeueCount)
	hash[MessageFieldEffectiveScheduledDelay.Key()] = strconv.FormatInt(t.EffectiveScheduledDelay, 10)

	// Optional timestamps
	setOptionalTS(hash, MessageFieldScheduledAt, t.ScheduledAt)
	setOptionalTS(hash, MessageFieldPublishedAt, t.PublishedAt)
	setOptionalTS(hash, MessageFieldRequeuedAt, t.RequeuedAt)
	setOptionalTS(hash, MessageFieldProcessingStartedAt, t.ProcessingStartedAt)
	setOptionalTS(hash, MessageFieldAcknowledgedAt, t.AcknowledgedAt)
	setOptionalTS(hash, MessageFieldUnacknowledgedAt, t.UnacknowledgedAt)
	setOptionalTS(hash, MessageFieldDeadLetteredAt, t.DeadLetteredAt)
	setOptionalTS(hash, MessageFieldLastRequeuedAt, t.LastRequeuedAt)
	setOptionalTS(hash, MessageFieldLastUnacknowledgedAt, t.LastUnacknowledgedAt)
	setOptionalTS(hash, MessageFieldLastScheduledAt, t.LastScheduledAt)
	setOptionalTS(hash, MessageFieldLastRetriedAttemptAt, t.LastRetriedAttemptAt)
	setOptionalTS(hash, MessageFieldLastProcessedAt, t.LastProcessedAt)

	// Optional parent IDs
	if t.ScheduledMessageParentID != "" {
		hash[MessageFieldScheduledMessageParentID.Key()] = t.ScheduledMessageParentID
	}
	if t.RequeuedMessageParentID != "" {
		hash[MessageFieldRequeuedMessageParentID.Key()] = t.RequeuedMessageParentID
	}

	return hash, nil
}

// DecodeHash deserializes a Redis hash map to State.
func (c *StateCodec) DecodeHash(_ context.Context, hash map[string]string) (*publicmessage.State, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode message state: empty hash")
	}

	state := publicmessage.NewMessageState()

	// Core properties
	if v, ok := hash[MessageFieldID.Key()]; ok {
		state.SetID(v)
	}
	if v, ok := hash[MessageFieldAttempts.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetAttempts(n)
	}
	if v, ok := hash[MessageFieldExpired.Key()]; ok {
		state.SetExpired(v == "1")
	}
	if v, ok := hash[MessageFieldScheduledCronFired.Key()]; ok {
		state.SetScheduledCronFired(v == "1")
	}
	if v, ok := hash[MessageFieldScheduledRepeatCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledRepeatCount(n)
	}
	if v, ok := hash[MessageFieldScheduledTimes.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledTimes(n)
	}
	if v, ok := hash[MessageFieldRequeueCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetRequeueCount(n)
	}
	if v, ok := hash[MessageFieldEffectiveScheduledDelay.Key()]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		state.SetEffectiveScheduledDelay(n)
	}

	// Optional timestamps
	parseOptionalTS(hash, MessageFieldScheduledAt, state.SetScheduledAt)
	parseOptionalTS(hash, MessageFieldPublishedAt, state.SetPublishedAt)
	parseOptionalTS(hash, MessageFieldRequeuedAt, state.SetRequeuedAt)
	parseOptionalTS(hash, MessageFieldProcessingStartedAt, state.SetProcessingStartedAt)
	parseOptionalTS(hash, MessageFieldAcknowledgedAt, state.SetAcknowledgedAt)
	parseOptionalTS(hash, MessageFieldUnacknowledgedAt, state.SetUnacknowledgedAt)
	parseOptionalTS(hash, MessageFieldDeadLetteredAt, state.SetDeadLetteredAt)
	parseOptionalTS(hash, MessageFieldLastRequeuedAt, state.SetLastRequeuedAt)
	parseOptionalTS(hash, MessageFieldLastUnacknowledgedAt, state.SetLastUnacknowledgedAt)
	parseOptionalTS(hash, MessageFieldLastScheduledAt, state.SetLastScheduledAt)
	parseOptionalTS(hash, MessageFieldLastRetriedAttemptAt, state.SetLastRetriedAttemptAt)
	parseOptionalTS(hash, MessageFieldLastProcessedAt, state.SetLastProcessedAt)

	// Optional parent IDs
	if v, ok := hash[MessageFieldScheduledMessageParentID.Key()]; ok {
		state.SetScheduledMessageParentID(v)
	}
	if v, ok := hash[MessageFieldRequeuedMessageParentID.Key()]; ok {
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

	params := env.ToParams()
	paramsJSON, err := c.params.Encode(ctx, params)
	if err != nil {
		return nil, err
	}

	hash := map[string]interface{}{
		MessageFieldMessage.Key(): string(paramsJSON),
		MessageFieldStatus.Key():  strconv.Itoa(env.Status().Int()),
	}

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

	paramsJSON := hash[MessageFieldMessage.Key()]
	params, err := c.params.Decode(ctx, []byte(paramsJSON))
	if err != nil {
		return nil, err
	}

	statusStr := hash[MessageFieldStatus.Key()]
	status, _ := strconv.Atoi(statusStr)

	stateHash := make(map[string]string)
	for k, v := range hash {
		if k != MessageFieldMessage.Key() && k != MessageFieldStatus.Key() {
			stateHash[k] = v
		}
	}
	state, err := c.state.DecodeHash(ctx, stateHash)
	if err != nil {
		return nil, err
	}

	message := rebuildMessage(params)
	if params.ScheduledDelay != nil {
		state.SetEffectiveScheduledDelay(*params.ScheduledDelay)
	}

	envelope := NewEnvelope(message)
	envelope.SetMessageState(state)
	envelope.SetStatus(publicmessage.Status(status))
	envelope.SetDestinationQueue(params.DestinationQueue)
	if params.ConsumerGroupID != "" {
		envelope.SetConsumerGroupID(params.ConsumerGroupID)
	}

	return envelope, nil
}

// rebuildMessage creates a ProducibleMessage from deserialized params.
func rebuildMessage(params *publicmessage.Params) *publicmessage.ProducibleMessage {
	msg := publicmessage.New()
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
		case exchange.TypeDirect:
			msg.SetDirectExchange(params.Exchange)
		case exchange.TypeFanout:
			msg.SetFanoutExchange(params.Exchange)
		case exchange.TypeTopic:
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

func setOptionalTS(hash map[string]interface{}, prop Field, ts *int64) {
	if ts != nil && *ts > 0 {
		hash[prop.Key()] = strconv.FormatInt(*ts, 10)
	}
}

func parseOptionalTS(hash map[string]string, prop Field, setter func(int64)) {
	if v, ok := hash[prop.Key()]; ok {
		ts, err := strconv.ParseInt(v, 10, 64)
		if err == nil && ts > 0 {
			setter(ts)
		}
	}
}
