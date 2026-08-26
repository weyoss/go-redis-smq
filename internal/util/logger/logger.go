/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	appconfig "github.com/weyoss/go-redis-smq/pkg/config"
)

// Config holds the logger settings that can change at runtime.
type Config struct {
	Enabled          bool
	Level            slog.Level
	Colorize         bool
	IncludeTimestamp bool
}

var closed atomic.Bool

// Shutdown disables all logging. Call before config.Close() to prevent panics
// from goroutines that outlive the configuration.
func Shutdown() {
	closed.Store(true)
}

// currentConfig returns the logger settings from the public configuration
// snapshot. If logging is shut down, it returns a disabled config.
func currentConfig() Config {
	if closed.Load() {
		return Config{Enabled: false}
	}

	c := appconfig.Get()
	level := slog.Level(c.Logger.Options.LogLevel)
	if level < slog.LevelDebug || level > slog.LevelError {
		level = slog.LevelInfo
	}

	return Config{
		Enabled:          c.Logger.Enabled,
		Level:            level,
		Colorize:         c.Logger.Options.Colorize,
		IncludeTimestamp: c.Logger.Options.IncludeTimestamp,
	}
}

var levelColors = map[slog.Level]string{
	slog.LevelDebug: "\u001b[36m",
	slog.LevelInfo:  "\u001b[32m",
	slog.LevelWarn:  "\u001b[33m",
	slog.LevelError: "\u001b[31m",
}

const resetColor = "\u001b[0m"

// Handler is a custom slog.Handler that writes human-readable colored output.
type Handler struct {
	mu     sync.Mutex
	writer io.Writer
	groups []string
	attrs  []slog.Attr
}

// Enabled reports whether the handler will emit records at the given level.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	if closed.Load() {
		return false
	}
	cfg := currentConfig()
	if !cfg.Enabled {
		return false
	}
	return level >= cfg.Level
}

// Handle formats and writes a log record.
func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	if closed.Load() {
		return nil
	}
	cfg := currentConfig()
	if !cfg.Enabled {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	var b strings.Builder

	if cfg.IncludeTimestamp {
		b.WriteString("[")
		b.WriteString(r.Time.Format("2006-01-02T15:04:05.000"))
		b.WriteString("] ")
	}

	b.WriteString(r.Level.String())

	if len(h.groups) > 0 {
		b.WriteString(" (")
		b.WriteString(strings.Join(h.groups, " / "))
		b.WriteString(")")
	}

	b.WriteString(": ")
	b.WriteString(r.Message)

	for _, a := range h.attrs {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
	}

	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" ")
		b.WriteString(a.Key)
		b.WriteString("=")
		b.WriteString(a.Value.String())
		return true
	})

	line := b.String()
	if cfg.Colorize {
		if color, ok := levelColors[r.Level]; ok {
			line = color + line + resetColor
		}
	}

	_, err := fmt.Fprintln(h.writer, line)
	return err
}

// WithAttrs returns a new Handler with the given attributes added.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := h.clone()
	h2.attrs = append(h2.attrs, attrs...)
	return h2
}

// WithGroup returns a new Handler with the given group name added.
func (h *Handler) WithGroup(name string) slog.Handler {
	h2 := h.clone()
	h2.groups = append(h2.groups, name)
	return h2
}

// clone creates a shallow copy with independent slices to avoid data races
// and unintended mutation of shared backing arrays.
func (h *Handler) clone() *Handler {
	h2 := &Handler{
		writer: h.writer,
		groups: make([]string, len(h.groups)),
		attrs:  make([]slog.Attr, len(h.attrs)),
	}
	copy(h2.groups, h.groups)
	copy(h2.attrs, h.attrs)
	return h2
}

// New creates a new logger that writes to os.Stdout.
func New(namespaces ...string) *slog.Logger {
	return NewWithWriter(os.Stdout, namespaces...)
}

// NewWithWriter creates a new logger with a custom io.Writer.
func NewWithWriter(w io.Writer, namespaces ...string) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	handler := &Handler{writer: w}
	for _, ns := range namespaces {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			ns = "unknown"
		}
		handler.groups = append(handler.groups, strings.ToLower(ns))
	}
	return slog.New(handler)
}
