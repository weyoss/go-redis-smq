package cfg

import (
	"log/slog"

	"github.com/weyoss/go-redis-smq/internal/config"
	"github.com/weyoss/go-redis-smq/internal/util/logger"
)

// Provider returns a logger.ConfigProvider backed by the internal
// configuration manager.
func Provider() logger.ConfigProvider {
	return adapter{}
}

type adapter struct{}

func (adapter) Get() logger.Config {
	c := config.Get()
	level := slog.Level(c.Logger.Options.LogLevel)
	if level < slog.LevelDebug || level > slog.LevelError {
		level = slog.LevelInfo
	}
	return logger.Config{
		Enabled:          c.Logger.Enabled,
		Level:            level,
		Colorize:         c.Logger.Options.Colorize,
		IncludeTimestamp: c.Logger.Options.IncludeTimestamp,
	}
}
