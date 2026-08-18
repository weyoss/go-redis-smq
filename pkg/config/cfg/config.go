/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

// Package cfg defines the public configuration structures used by RedisSMQ.
//
// These types are returned by config.Get() and can be modified before being
// passed to config.Save().
package cfg

// Config holds the application configuration.
type Config struct {
	// Version is the current configuration version. It is managed
	// automatically and should not be set by callers.
	Version int `json:"-"`

	// Namespace is the default namespace used when no explicit namespace is
	// provided during queue or exchange operations.
	Namespace string `json:"namespace"`

	// Logger contains console logging settings.
	Logger LoggerConfig `json:"logger"`

	// MessageAudit contains message audit settings.
	MessageAudit MessageAudit `json:"messageAudit"`
}

// LoggerConfig controls console output.
type LoggerConfig struct {
	// Enabled enables or disables console logging.
	Enabled bool `json:"enabled"`

	// Options contains logger formatting and level options.
	Options LoggerOptionsConfig `json:"options"`
}

// LoggerOptionsConfig contains logger formatting options.
type LoggerOptionsConfig struct {
	// IncludeTimestamp includes timestamps in log output.
	IncludeTimestamp bool `json:"includeTimestamp"`

	// Colorize enables ANSI color coding in log output.
	Colorize bool `json:"colorize"`

	// LogLevel is the minimum log level to output:
	// 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR.
	LogLevel int `json:"logLevel"`
}

// MessageAudit controls message audit trails.
type MessageAudit struct {
	// AcknowledgedMessages configures audit for successfully processed
	// messages.
	AcknowledgedMessages AuditMessagesConfig `json:"acknowledgedMessages"`

	// DeadLetteredMessages configures audit for permanently failed messages.
	DeadLetteredMessages AuditMessagesConfig `json:"deadLetteredMessages"`

	// UnacknowledgementHistory configures per-message failure history.
	UnacknowledgementHistory AuditHistoryConfig `json:"unacknowledgementHistory"`
}

// AuditMessagesConfig configures storage for processed messages.
type AuditMessagesConfig struct {
	// Enabled enables or disables the audit category.
	Enabled bool `json:"enabled"`

	// QueueSize is the maximum number of messages to keep per queue.
	// A value of 0 means unlimited.
	QueueSize int `json:"queueSize"`

	// Expire is the retention time in seconds. A value of 0 means never
	// expire.
	Expire int `json:"expire"`
}

// AuditHistoryConfig configures unacknowledgment history storage.
type AuditHistoryConfig struct {
	// Enabled enables or disables per-message unacknowledgment history.
	Enabled bool `json:"enabled"`

	// MaxSize is the maximum number of history records per message.
	// A value of 0 means unlimited.
	MaxSize int `json:"maxSize"`
}

// DefaultConfig returns the factory default configuration.
//
// The defaults are:
//   - Namespace: "default"
//   - Logger: disabled, timestamps enabled, colors enabled, level INFO
//   - Message audit: all categories disabled
//   - Unacknowledgment history max size: 100
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
