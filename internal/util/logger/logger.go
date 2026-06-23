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
)

// Config holds the logger settings that can change at runtime.
type Config struct {
	Enabled          bool
	Level            slog.Level
	Colorize         bool
	IncludeTimestamp bool
}

// ConfigProvider allows the logger to read settings dynamically
// without depending on any concrete configuration source.
type ConfigProvider interface {
	Get() Config
}

var (
	providerMu sync.RWMutex
	provider   ConfigProvider = defaultProvider{}
	closed     atomic.Bool
)

// Init sets the configuration source for all loggers created by this package.
// Safe to call multiple times — resets the shutdown flag for re-initialization.
func Init(p ConfigProvider) {
	closed.Store(false)
	providerMu.Lock()
	defer providerMu.Unlock()
	provider = p
}

// Shutdown disables all logging. Call before config.Close() to prevent panics
// from goroutines that outlive the configuration.
func Shutdown() {
	closed.Store(true)
}

func getConfig() Config {
	if closed.Load() {
		return Config{Enabled: false}
	}
	providerMu.RLock()
	defer providerMu.RUnlock()
	return provider.Get()
}

type defaultProvider struct{}

func (defaultProvider) Get() Config {
	return Config{
		Enabled:          false,
		Level:            slog.LevelInfo,
		Colorize:         true,
		IncludeTimestamp: true,
	}
}

// ── slog.Handler implementation ──

var levelColors = map[slog.Level]string{
	slog.LevelDebug: "\u001b[36m",
	slog.LevelInfo:  "\u001b[32m",
	slog.LevelWarn:  "\u001b[33m",
	slog.LevelError: "\u001b[31m",
}

const resetColor = "\u001b[0m"

type Handler struct {
	mu     sync.Mutex
	writer io.Writer
	groups []string
	attrs  []slog.Attr
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	if closed.Load() {
		return false
	}
	cfg := getConfig()
	if !cfg.Enabled {
		return false
	}
	return level >= cfg.Level
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	if closed.Load() {
		return nil
	}
	cfg := getConfig()
	if !cfg.Enabled {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	var parts []string
	if cfg.IncludeTimestamp {
		parts = append(parts, "["+r.Time.Format("2006-01-02T15:04:05.000")+"]")
	}
	parts = append(parts, r.Level.String())
	if len(h.groups) > 0 {
		parts = append(parts, "("+strings.Join(h.groups, " / ")+")")
	}
	base := strings.Join(parts, " ") + ": " + r.Message

	r.Attrs(func(a slog.Attr) bool {
		base += " " + a.Key + "=" + a.Value.String()
		return true
	})

	if cfg.Colorize {
		if color, ok := levelColors[r.Level]; ok {
			base = color + base + resetColor
		}
	}

	fmt.Fprintln(h.writer, base)
	return nil
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = append(h.attrs, attrs...)
	return &h2
}

func (h *Handler) WithGroup(name string) slog.Handler {
	h2 := *h
	h2.groups = append(h2.groups, name)
	return &h2
}

// ── Public constructor ──

// New creates a new logger that dynamically reads its settings
// from the ConfigProvider set via Init.
func New(namespaces ...string) *slog.Logger {
	handler := &Handler{writer: os.Stdout}
	for _, ns := range namespaces {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			ns = "unknown"
		}
		handler.groups = append(handler.groups, strings.ToLower(ns))
	}
	return slog.New(handler)
}
