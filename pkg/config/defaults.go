/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package config

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
