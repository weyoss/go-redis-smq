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

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

//go:embed all:scripts
var FS embed.FS

// scriptDefs maps each script ID to the ordered list of Lua files that compose it.
// A single file means no concatenation. Multiple files are concatenated in order —
// shared procedures first, then the main script.
var scriptDefs = map[ID][]string{
	// Core commands
	PublishScheduled: {
		"scripts/core/shared-procedures/publish-message.lua",
		"scripts/core/publish-scheduled.lua",
	},
	PublishMessage: {
		"scripts/core/shared-procedures/publish-message.lua",
		"scripts/core/publish-message.lua",
	},
	RequeueMessage: {
		"scripts/core/shared-procedures/publish-message.lua",
		"scripts/core/requeue-message.lua",
	},
	RequeueImmediate:     {"scripts/core/requeue-immediate.lua"},
	RequeueDelayed:       {"scripts/core/requeue-delayed.lua"},
	CreateQueue:          {"scripts/core/create-queue.lua"},
	SubscribeConsumer:    {"scripts/core/subscribe-consumer.lua"},
	UnsubscribeConsumer:  {"scripts/core/unsubscribe-consumer.lua"},
	UnacknowledgeMessage: {"scripts/core/unacknowledge-message.lua"},
	AcknowledgeMessage:   {"scripts/core/acknowledge-message.lua"},
	DeleteMessage:        {"scripts/core/delete-message.lua"},
	CheckoutMessage:      {"scripts/core/checkout-message.lua"},
	DeleteConsumerGroup:  {"scripts/core/delete-consumer-group.lua"},
	CheckRateLimit:       {"scripts/core/check-queue-rate-limit.lua"},
	SetRateLimit:         {"scripts/core/set-queue-rate-limit.lua"},
	DeleteQueue:          {"scripts/core/delete-queue.lua"},
	ClearRateLimit:       {"scripts/core/clear-queue-rate-limit.lua"},
	SetQueueState:        {"scripts/core/set-queue-state.lua"},
	GetQueueState:        {"scripts/core/get-queue-state.lua"},
	SaveConfig:           {"scripts/core/save-config.lua"},

	// Custom Redis command
	ZPOPLPUSH: {"scripts/commands/zpoplpush.lua"},

	// Lock
	ExtendLock:  {"scripts/redis_lock/extend-lock.lua"},
	ReleaseLock: {"scripts/redis_lock/release-lock.lua"},

	// Jobs
	CreateJob:       {"scripts/jobs/create-job.lua"},
	StartJob:        {"scripts/jobs/start-job.lua"},
	CompleteJob:     {"scripts/jobs/complete-job.lua"},
	FailJob:         {"scripts/jobs/fail-job.lua"},
	CancelJob:       {"scripts/jobs/cancel-job.lua"},
	RecoverStuckJob: {"scripts/jobs/recover-stuck-job.lua"},
}

// ScriptManager loads Lua scripts from the embedded filesystem and registers
// them with Redis for SHA-based execution.
type ScriptManager struct {
	client   *redis.Client
	contents map[ID]string // concatenated Lua source per script
	shas     map[ID]string // Redis SHA per script
}

// NewScriptManager creates a manager bound to the given Redis client.
func NewScriptManager(client *redis.Client) *ScriptManager {
	return &ScriptManager{
		client:   client,
		contents: make(map[ID]string),
		shas:     make(map[ID]string),
	}
}

// RegisterAll reads, concatenates, and loads every defined script into Redis.
// Must be called once during bootstrap, after Redis connectivity is confirmed.
func (sm *ScriptManager) RegisterAll(ctx context.Context) error {
	for id, files := range scriptDefs {
		source, err := concatFiles(files)
		if err != nil {
			return fmt.Errorf("read script %s: %w", id, err)
		}
		sm.contents[id] = source

		sha, err := sm.client.ScriptLoad(ctx, source).Result()
		if err != nil {
			return fmt.Errorf("register script %s: %w", id, err)
		}
		sm.shas[id] = sha
	}
	return nil
}

// Eval executes a registered Lua script by ID.
// Uses the cached SHA for efficiency; falls back to EVAL on NOSCRIPT errors.
func (sm *ScriptManager) Eval(ctx context.Context, id ID, keys []string, args ...interface{}) (interface{}, error) {
	if sha, ok := sm.shas[id]; ok {
		result, err := sm.client.EvalSha(ctx, sha, keys, args...).Result()
		if err == nil {
			return result, nil
		}
		if !strings.Contains(err.Error(), "NOSCRIPT") {
			return nil, fmt.Errorf("execute script %s: %w", id, err)
		}
	}

	source, ok := sm.contents[id]
	if !ok {
		return nil, fmt.Errorf("script %s not registered", id)
	}
	return sm.client.Eval(ctx, source, keys, args...).Result()
}

// concatFiles reads the given files from the embedded FS and joins their contents.
func concatFiles(files []string) (string, error) {
	parts := make([]string, 0, len(files))
	for _, path := range files {
		data, err := FS.ReadFile(path)
		if err != nil {
			return "", err
		}
		parts = append(parts, string(data))
	}
	return strings.Join(parts, "\n"), nil
}
