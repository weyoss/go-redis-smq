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
	"github.com/weyoss/go-redis-smq/pkg/exchange"
	publicmessage "github.com/weyoss/go-redis-smq/pkg/message"
)

// Codec handles serialisation for message Params, State, and Envelope.
type Codec struct{}

// NewCodec creates a new Codec.
func NewCodec() *Codec {
	return &Codec{}
}

// EncodeParams serialises message Params to JSON bytes.
func (c *Codec) EncodeParams(_ context.Context, params *publicmessage.Params) ([]byte, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encode message params: %w", err)
	}
	return data, nil
}

// DecodeParams deserialises JSON bytes back to Params.
func (c *Codec) DecodeParams(_ context.Context, data []byte) (*publicmessage.Params, error) {
	var params publicmessage.Params
	if err := json.Unmarshal(data, &params); err != nil {
		return nil, fmt.Errorf("decode message params: %w", err)
	}
	return &params, nil
}

// EncodeState serialises State to a Redis hash map.
func (c *Codec) EncodeState(_ context.Context, state *publicmessage.State) (map[string]string, error) {
	if state == nil {
		return nil, fmt.Errorf("encode message state: nil")
	}

	t := state.ToTransferable()
	hash := map[string]string{
		schema.ID.Key():                      t.UUID,
		schema.Attempts.Key():                strconv.Itoa(t.Attempts),
		schema.Expired.Key():                 boolToStr(t.Expired),
		schema.ScheduledCronFired.Key():      boolToStr(t.ScheduledCronFired),
		schema.ScheduledRepeatCount.Key():    strconv.Itoa(t.ScheduledRepeatCount),
		schema.ScheduledTimes.Key():          strconv.Itoa(t.ScheduledTimes),
		schema.RequeueCount.Key():            strconv.Itoa(t.RequeueCount),
		schema.EffectiveScheduledDelay.Key(): strconv.FormatInt(t.EffectiveScheduledDelay, 10),
	}

	setOptionalTS(hash, schema.ScheduledAt, t.ScheduledAt)
	setOptionalTS(hash, schema.PublishedAt, t.PublishedAt)
	setOptionalTS(hash, schema.RequeuedAt, t.RequeuedAt)
	setOptionalTS(hash, schema.ProcessingStartedAt, t.ProcessingStartedAt)
	setOptionalTS(hash, schema.AcknowledgedAt, t.AcknowledgedAt)
	setOptionalTS(hash, schema.UnacknowledgedAt, t.UnacknowledgedAt)
	setOptionalTS(hash, schema.DeadLetteredAt, t.DeadLetteredAt)
	setOptionalTS(hash, schema.LastRequeuedAt, t.LastRequeuedAt)
	setOptionalTS(hash, schema.LastUnacknowledgedAt, t.LastUnacknowledgedAt)
	setOptionalTS(hash, schema.LastScheduledAt, t.LastScheduledAt)
	setOptionalTS(hash, schema.LastRetriedAttemptAt, t.LastRetriedAttemptAt)
	setOptionalTS(hash, schema.LastProcessedAt, t.LastProcessedAt)

	if t.ScheduledMessageParentID != "" {
		hash[schema.ScheduledMessageParentID.Key()] = t.ScheduledMessageParentID
	}
	if t.RequeuedMessageParentID != "" {
		hash[schema.RequeuedMessageParentID.Key()] = t.RequeuedMessageParentID
	}

	return hash, nil
}

// DecodeState deserialises a Redis hash map back to State.
func (c *Codec) DecodeState(_ context.Context, hash map[string]string) (*publicmessage.State, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode message state: empty hash")
	}

	state := publicmessage.NewMessageState()

	if v, ok := hash[schema.ID.Key()]; ok {
		state.SetID(v)
	}
	if v, ok := hash[schema.Attempts.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetAttempts(n)
	}
	if v, ok := hash[schema.Expired.Key()]; ok {
		state.SetExpired(v == "1")
	}
	if v, ok := hash[schema.ScheduledCronFired.Key()]; ok {
		state.SetScheduledCronFired(v == "1")
	}
	if v, ok := hash[schema.ScheduledRepeatCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledRepeatCount(n)
	}
	if v, ok := hash[schema.ScheduledTimes.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetScheduledTimes(n)
	}
	if v, ok := hash[schema.RequeueCount.Key()]; ok {
		n, _ := strconv.Atoi(v)
		state.SetRequeueCount(n)
	}
	if v, ok := hash[schema.EffectiveScheduledDelay.Key()]; ok {
		n, _ := strconv.ParseInt(v, 10, 64)
		state.SetEffectiveScheduledDelay(n)
	}

	// Optional timestamps
	parseOptionalTS(hash, schema.ScheduledAt, state.SetScheduledAt)
	parseOptionalTS(hash, schema.PublishedAt, state.SetPublishedAt)
	parseOptionalTS(hash, schema.RequeuedAt, state.SetRequeuedAt)
	parseOptionalTS(hash, schema.ProcessingStartedAt, state.SetProcessingStartedAt)
	parseOptionalTS(hash, schema.AcknowledgedAt, state.SetAcknowledgedAt)
	parseOptionalTS(hash, schema.UnacknowledgedAt, state.SetUnacknowledgedAt)
	parseOptionalTS(hash, schema.DeadLetteredAt, state.SetDeadLetteredAt)
	parseOptionalTS(hash, schema.LastRequeuedAt, state.SetLastRequeuedAt)
	parseOptionalTS(hash, schema.LastUnacknowledgedAt, state.SetLastUnacknowledgedAt)
	parseOptionalTS(hash, schema.LastScheduledAt, state.SetLastScheduledAt)
	parseOptionalTS(hash, schema.LastRetriedAttemptAt, state.SetLastRetriedAttemptAt)
	parseOptionalTS(hash, schema.LastProcessedAt, state.SetLastProcessedAt)

	// Optional parent IDs
	if v, ok := hash[schema.ScheduledMessageParentID.Key()]; ok {
		state.SetScheduledMessageParentID(v)
	}
	if v, ok := hash[schema.RequeuedMessageParentID.Key()]; ok {
		state.SetRequeuedMessageParentID(v)
	}

	return state, nil
}

// EncodeEnvelope serialises an Envelope to a Redis hash map.
func (c *Codec) EncodeEnvelope(ctx context.Context, env *Envelope) (map[string]string, error) {
	if env == nil {
		return nil, fmt.Errorf("encode message envelope: nil")
	}

	params := env.ToParams()
	paramsJSON, err := c.EncodeParams(ctx, params)
	if err != nil {
		return nil, err
	}

	hash := map[string]string{
		schema.Message.Key(): string(paramsJSON),
		schema.Status.Key():  strconv.Itoa(env.Status().Int()),
	}

	stateHash, err := c.EncodeState(ctx, env.MessageState())
	if err != nil {
		return nil, err
	}
	for k, v := range stateHash {
		hash[k] = v
	}

	return hash, nil
}

// DecodeEnvelope deserialises a Redis hash map back to an Envelope.
func (c *Codec) DecodeEnvelope(ctx context.Context, hash map[string]string) (*Envelope, error) {
	if len(hash) == 0 {
		return nil, fmt.Errorf("decode message envelope: empty hash")
	}

	paramsJSON := hash[schema.Message.Key()]
	params, err := c.DecodeParams(ctx, []byte(paramsJSON))
	if err != nil {
		return nil, err
	}

	statusStr := hash[schema.Status.Key()]
	status, _ := strconv.Atoi(statusStr)

	stateHash := make(map[string]string)
	for k, v := range hash {
		if k != schema.Message.Key() && k != schema.Status.Key() {
			stateHash[k] = v
		}
	}
	state, err := c.DecodeState(ctx, stateHash)
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

// rebuildMessage creates a ProducibleMessage from deserialised params.
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

func setOptionalTS(hash map[string]string, prop schema.Field, ts *int64) {
	if ts != nil && *ts > 0 {
		hash[prop.Key()] = strconv.FormatInt(*ts, 10)
	}
}

func parseOptionalTS(hash map[string]string, prop schema.Field, setter func(int64)) {
	if v, ok := hash[prop.Key()]; ok {
		ts, err := strconv.ParseInt(v, 10, 64)
		if err == nil && ts > 0 {
			setter(ts)
		}
	}
}
