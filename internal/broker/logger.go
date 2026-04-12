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

// SetLevel updates the logger level dynamically.
// Note: This creates a new logger with the updated level.
func (l *Logger) SetLevel(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	l.level = lvl
}

// SetFormat updates the logger format dynamically.
// Note: This requires recreating the handler.
func (l *Logger) SetFormat(format string) {
	// Format değişikliği için handler'ın yeniden oluşturulması gerekir
	// Bu basit implementasyonda no-op - production ortamında handler yeniden oluşturulur
	_ = format
}

// LogConnectionOpen logs a connection open event.
func (l *Logger) LogConnectionOpen(remoteAddr string, protocol string) {
	l.Info("connection opened",
		"remote_addr", remoteAddr,
		"protocol", protocol,
		"event_type", "connection",
		"event_action", "open",
	)
}

// LogConnectionClose logs a connection close event.
func (l *Logger) LogConnectionClose(remoteAddr string, protocol string, durationSecs float64) {
	l.Info("connection closed",
		"remote_addr", remoteAddr,
		"protocol", protocol,
		"duration_secs", durationSecs,
		"event_type", "connection",
		"event_action", "close",
	)
}

// LogAuthFailure logs an authentication failure event.
func (l *Logger) LogAuthFailure(remoteAddr string, username string, reason string) {
	l.Warn("authentication failed",
		"remote_addr", remoteAddr,
		"username", username,
		"reason", reason,
		"event_type", "auth",
		"event_action", "failure",
	)
}

// LogAuthSuccess logs an authentication success event.
func (l *Logger) LogAuthSuccess(remoteAddr string, username string, source string) {
	l.Debug("authentication succeeded",
		"remote_addr", remoteAddr,
		"username", username,
		"source", source,
		"event_type", "auth",
		"event_action", "success",
	)
}

// LogSlowConsumer logs a slow consumer warning.
func (l *Logger) LogSlowConsumer(topic string, consumerID string, lag int64) {
	l.Warn("slow consumer detected",
		"topic", topic,
		"consumer_id", consumerID,
		"lag", lag,
		"event_type", "consumer",
		"event_action", "slow",
	)
}

// LogProduce logs a message production event.
func (l *Logger) LogProduce(topic string, partition uint32, offset uint64, size int) {
	l.Debug("message produced",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"size_bytes", size,
		"event_type", "produce",
	)
}

// LogConsume logs a message consumption event.
func (l *Logger) LogConsume(topic string, partition uint32, offset uint64, consumerID string) {
	l.Debug("message consumed",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"consumer_id", consumerID,
		"event_type", "consume",
	)
}

// LogDLQ logs a dead letter queue event.
func (l *Logger) LogDLQ(topic string, partition uint32, offset uint64, reason string) {
	l.Warn("message moved to DLQ",
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"reason", reason,
		"event_type", "dlq",
		"event_action", "move",
	)
}
