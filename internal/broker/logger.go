package broker

import (
	"log/slog"
	"os"
)

// Logger wraps slog for structured logging.
type Logger struct {
	inner *slog.Logger
	level slog.Level
}

// NewLogger creates a logger from LoggingConfig.
func NewLogger(cfg LoggingConfig) *Logger {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var writer = os.Stdout
	if cfg.Output == "file" && cfg.File != "" {
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err == nil {
			writer = f
		}
	}

	var handler slog.Handler
	if cfg.Format == "text" {
		handler = slog.NewTextHandler(writer, opts)
	} else {
		handler = slog.NewJSONHandler(writer, opts)
	}

	return &Logger{
		inner: slog.New(handler),
		level: level,
	}
}

func (l *Logger) Debug(msg string, args ...any) { l.inner.Debug(msg, args...) }
func (l *Logger) Info(msg string, args ...any)  { l.inner.Info(msg, args...) }
func (l *Logger) Warn(msg string, args ...any)  { l.inner.Warn(msg, args...) }
func (l *Logger) Error(msg string, args ...any) { l.inner.Error(msg, args...) }
func (l *Logger) With(args ...any) *Logger {
	return &Logger{inner: l.inner.With(args...), level: l.level}
}
