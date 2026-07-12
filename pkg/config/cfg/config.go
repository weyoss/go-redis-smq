/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package cfg

// Config holds the application configuration.
type Config struct {
	Version      int          `json:"-"`
	Namespace    string       `json:"namespace"`
	Logger       LoggerConfig `json:"logger"`
	MessageAudit MessageAudit `json:"messageAudit"`
}

type LoggerConfig struct {
	Enabled bool                `json:"enabled"`
	Options LoggerOptionsConfig `json:"options"`
}

type LoggerOptionsConfig struct {
	IncludeTimestamp bool `json:"includeTimestamp"`
	Colorize         bool `json:"colorize"`
	LogLevel         int  `json:"logLevel"`
}

type MessageAudit struct {
	AcknowledgedMessages     AuditMessagesConfig `json:"acknowledgedMessages"`
	DeadLetteredMessages     AuditMessagesConfig `json:"deadLetteredMessages"`
	UnacknowledgementHistory AuditHistoryConfig  `json:"unacknowledgementHistory"`
}

type AuditMessagesConfig struct {
	Enabled   bool `json:"enabled"`
	QueueSize int  `json:"queueSize"`
	Expire    int  `json:"expire"`
}

type AuditHistoryConfig struct {
	Enabled bool `json:"enabled"`
	MaxSize int  `json:"maxSize"`
}

func DefaultConfig() *Config {
	return &Config{
		Namespace: "default",
		Logger: LoggerConfig{
			Enabled: false,
			Options: LoggerOptionsConfig{
				IncludeTimestamp: true,
				Colorize:         true,
				LogLevel:         1,
			},
		},
		MessageAudit: MessageAudit{
			AcknowledgedMessages: AuditMessagesConfig{
				Enabled:   false,
				QueueSize: 0,
				Expire:    0,
			},
			DeadLetteredMessages: AuditMessagesConfig{
				Enabled:   false,
				QueueSize: 0,
				Expire:    0,
			},
			UnacknowledgementHistory: AuditHistoryConfig{
				Enabled: false,
				MaxSize: 100,
			},
		},
	}
}
