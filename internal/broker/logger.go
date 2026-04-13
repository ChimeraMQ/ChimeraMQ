package broker

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Logger wraps slog for structured logging with rotation support.
type Logger struct {
	inner  *slog.Logger
	level  slog.Level
	config LoggingConfig
	file   *os.File
	mu     sync.Mutex
	size   int64
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

	logger := &Logger{
		config: cfg,
		level:  level,
	}

	var writer io.Writer = os.Stdout
	if cfg.Output == "file" && cfg.File != "" {
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err == nil {
			writer = f
			logger.file = f
			// Get current file size
			if info, err := f.Stat(); err == nil {
				logger.size = info.Size()
			}
		}
	}

	var handler slog.Handler
	if cfg.Format == "text" {
		handler = slog.NewTextHandler(writer, opts)
	} else {
		handler = slog.NewJSONHandler(writer, opts)
	}

	logger.inner = slog.New(handler)
	return logger
}

// rotate performs log rotation if needed.
func (l *Logger) rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	// Check if rotation is needed
	if l.config.MaxSize > 0 && l.size >= l.config.MaxSize {
		return l.doRotate()
	}

	return nil
}

// doRotate performs the actual rotation.
func (l *Logger) doRotate() error {
	// Close current file
	l.file.Close()

	// Rename with timestamp
	timestamp := time.Now().Format("20060102-150405")
	backupPath := l.config.File + "." + timestamp
	if err := os.Rename(l.config.File, backupPath); err != nil {
		// Try to reopen original file
		f, _ := os.OpenFile(l.config.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		l.file = f
		return fmt.Errorf("failed to rotate log: %w", err)
	}

	// Open new file
	f, err := os.OpenFile(l.config.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return fmt.Errorf("failed to open new log file: %w", err)
	}

	l.file = f
	l.size = 0

	// Update handler
	opts := &slog.HandlerOptions{Level: l.level}
	var handler slog.Handler
	if l.config.Format == "text" {
		handler = slog.NewTextHandler(f, opts)
	} else {
		handler = slog.NewJSONHandler(f, opts)
	}
	l.inner = slog.New(handler)

	// Clean up old logs
	l.cleanupOldLogs()

	return nil
}

// cleanupOldLogs removes log files older than MaxAge days.
func (l *Logger) cleanupOldLogs() {
	if l.config.MaxAge <= 0 {
		return
	}

	dir := filepath.Dir(l.config.File)
	base := filepath.Base(l.config.File)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := time.Now().AddDate(0, 0, -l.config.MaxAge)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !isLogFile(entry.Name(), base) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
}

func isLogFile(name, base string) bool {
	// Check if it's a rotated log file
	return len(name) > len(base) && name[:len(base)] == base && name[len(base)] == '.'
}

// write wraps the actual write to track size.
func (l *Logger) write(fn func()) {
	l.mu.Lock()
	fn()
	l.mu.Unlock()

	// Check rotation outside lock to avoid holding lock during I/O
	if err := l.rotate(); err != nil {
		// Log rotation failed - write to stderr as fallback
		fmt.Fprintf(os.Stderr, "log rotation failed: %v\n", err)
	}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.write(func() { l.inner.Debug(msg, args...) })
}

func (l *Logger) Info(msg string, args ...any) {
	l.write(func() { l.inner.Info(msg, args...) })
}

func (l *Logger) Warn(msg string, args ...any) {
	l.write(func() { l.inner.Warn(msg, args...) })
}

func (l *Logger) Error(msg string, args ...any) {
	l.write(func() { l.inner.Error(msg, args...) })
}

func (l *Logger) With(args ...any) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &Logger{
		inner:  l.inner.With(args...),
		level:  l.level,
		config: l.config,
		file:   l.file,
		size:   l.size,
	}
}

// SetLevel updates the logger level dynamically.
func (l *Logger) SetLevel(level string) {
	l.mu.Lock()
	defer l.mu.Unlock()

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

	// Update handler with new level
	if l.file != nil {
		opts := &slog.HandlerOptions{Level: lvl}
		var handler slog.Handler
		if l.config.Format == "text" {
			handler = slog.NewTextHandler(l.file, opts)
		} else {
			handler = slog.NewJSONHandler(l.file, opts)
		}
		l.inner = slog.New(handler)
	}
}

// SetFormat updates the logger format dynamically.
func (l *Logger) SetFormat(format string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.config.Format = format

	// Update handler
	if l.file != nil {
		opts := &slog.HandlerOptions{Level: l.level}
		var handler slog.Handler
		if format == "text" {
			handler = slog.NewTextHandler(l.file, opts)
		} else {
			handler = slog.NewJSONHandler(l.file, opts)
		}
		l.inner = slog.New(handler)
	}
}

// Close closes the log file if open.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
