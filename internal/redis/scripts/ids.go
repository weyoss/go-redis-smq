/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package scripts

// ID identifies a Lua script registered with Redis.
type ID string

const (
	PublishScheduled     ID = "PUBLISH_SCHEDULED"
	PublishMessage       ID = "PUBLISH_MESSAGE"
	RequeueMessage       ID = "REQUEUE_MESSAGE"
	RequeueImmediate     ID = "REQUEUE_IMMEDIATE"
	RequeueDelayed       ID = "REQUEUE_DELAYED"
	CreateQueue          ID = "CREATE_QUEUE"
	SubscribeConsumer    ID = "SUBSCRIBE_CONSUMER"
	UnsubscribeConsumer  ID = "UNSUBSCRIBE_CONSUMER"
	UnacknowledgeMessage ID = "UNACKNOWLEDGE_MESSAGE"
	AcknowledgeMessage   ID = "ACKNOWLEDGE_MESSAGE"
	DeleteMessage        ID = "DELETE_MESSAGE"
	CheckoutMessage      ID = "CHECKOUT_MESSAGE"
	DeleteConsumerGroup  ID = "DELETE_CONSUMER_GROUP"
	CheckRateLimit       ID = "CHECK_QUEUE_RATE_LIMIT"
	SetRateLimit         ID = "SET_QUEUE_RATE_LIMIT"
	DeleteQueue          ID = "DELETE_QUEUE"
	ClearRateLimit       ID = "CLEAR_QUEUE_RATE_LIMIT"
	SetQueueState        ID = "SET_QUEUE_STATE"
	GetQueueState        ID = "GET_QUEUE_STATE"
	SaveConfig           ID = "SAVE_CONFIG"

	// ZPOPLPUSH Custom Lua script: atomically pop highest priority and push to processing
	ZPOPLPUSH ID = "ZPOPLPUSH"

	// Redis lock scripts
	ExtendLock  ID = "EXTEND_LOCK"
	ReleaseLock ID = "RELEASE_LOCK"

	//
	CreateJob       = "CREATE_JOB"
	StartJob        = "START_JOB"
	CompleteJob     = "COMPLETE_JOB"
	FailJob         = "FAIL_JOB"
	CancelJob       = "CANCEL_JOB"
	RecoverStuckJob = "RECOVER_STUCK_JOB"
)
