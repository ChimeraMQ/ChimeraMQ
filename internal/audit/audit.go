package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Level represents the audit log level.
type Level string

const (
	LevelInfo    Level = "INFO"
	LevelWarning Level = "WARNING"
	LevelError   Level = "ERROR"
)

// Category represents the audit event category.
type Category string

const (
	CategoryAuth     Category = "AUTH"
	CategoryAdmin    Category = "ADMIN"
	CategoryMessage  Category = "MESSAGE"
	CategoryCluster  Category = "CLUSTER"
	CategoryConfig   Category = "CONFIG"
	CategorySecurity Category = "SECURITY"
)

// Event represents a single audit event.
type Event struct {
	Timestamp  time.Time       `json:"timestamp"`
	Level      Level           `json:"level"`
	Category   Category        `json:"category"`
	Action     string          `json:"action"`
	User       string          `json:"user,omitempty"`
	RemoteAddr string          `json:"remote_addr,omitempty"`
	Resource   string          `json:"resource,omitempty"`
	ResourceID string          `json:"resource_id,omitempty"`
	Result     string          `json:"result"`
	Details    json.RawMessage `json:"details,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// Logger provides audit logging functionality.
type Logger struct {
	mu      sync.RWMutex
	enabled bool
	file    *os.File
	encoder *json.Encoder
	logPath string
	maxSize int64 // bytes
	maxAge  time.Duration
}

// Config holds audit logger configuration.
type Config struct {
	Enabled  bool
	LogPath  string
	MaxSize  int64 // bytes
	MaxAge   time.Duration
	ToStdout bool
}

// NewLogger creates a new audit logger.
func NewLogger(cfg Config) (*Logger, error) {
	if !cfg.Enabled {
		return &Logger{enabled: false}, nil
	}

	l := &Logger{
		enabled: true,
		logPath: cfg.LogPath,
		maxSize: cfg.MaxSize,
		maxAge:  cfg.MaxAge,
	}

	if cfg.MaxSize == 0 {
		l.maxSize = 100 * 1024 * 1024 // 100MB default
	}
	if cfg.MaxAge == 0 {
		l.maxAge = 30 * 24 * time.Hour // 30 days default
	}

	// Default log path
	if l.logPath == "" {
		l.logPath = "/var/log/chimera/audit.log"
	}

	// Create log directory if needed
	if !cfg.ToStdout {
		dir := filepath.Dir(l.logPath)
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, fmt.Errorf("create audit log directory: %w", err)
		}

		file, err := os.OpenFile(l.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
		if err != nil {
			return nil, fmt.Errorf("open audit log file: %w", err)
		}
		l.file = file
		l.encoder = json.NewEncoder(file)
	} else {
		l.encoder = json.NewEncoder(os.Stdout)
	}

	return l, nil
}

// Close closes the audit logger.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// Log records an audit event.
func (l *Logger) Log(event Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.enabled {
		return
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	_ = l.encoder.Encode(event)

	// Check rotation
	l.checkRotation()
}

// LogAuth records an authentication event.
func (l *Logger) LogAuth(user, remoteAddr, action, result string, err error) {
	event := Event{
		Level:      LevelInfo,
		Category:   CategoryAuth,
		Action:     action,
		User:       user,
		RemoteAddr: remoteAddr,
		Result:     result,
	}
	if err != nil {
		event.Level = LevelError
		event.Error = err.Error()
	}
	l.Log(event)
}

// LogAdmin records an administrative operation.
func (l *Logger) LogAdmin(user, remoteAddr, resource, resourceID, action, result string) {
	l.Log(Event{
		Level:      LevelInfo,
		Category:   CategoryAdmin,
		Action:     action,
		User:       user,
		RemoteAddr: remoteAddr,
		Resource:   resource,
		ResourceID: resourceID,
		Result:     result,
	})
}

// LogMessage records a message operation.
func (l *Logger) LogMessage(user, topic, action string, messageCount int, result string) {
	details, _ := json.Marshal(map[string]interface{}{
		"message_count": messageCount,
	})

	l.Log(Event{
		Level:      LevelInfo,
		Category:   CategoryMessage,
		Action:     action,
		User:       user,
		Resource:   "topic",
		ResourceID: topic,
		Result:     result,
		Details:    details,
	})
}

// LogCluster records a cluster operation.
func (l *Logger) LogCluster(nodeID, action, result string, details map[string]interface{}) {
	detailJSON, _ := json.Marshal(details)
	l.Log(Event{
		Level:      LevelInfo,
		Category:   CategoryCluster,
		Action:     action,
		Resource:   "node",
		ResourceID: nodeID,
		Result:     result,
		Details:    detailJSON,
	})
}

// LogConfig records a configuration change.
func (l *Logger) LogConfig(user, remoteAddr, component, action string, oldVal, newVal interface{}) {
	details, _ := json.Marshal(map[string]interface{}{
		"old_value": oldVal,
		"new_value": newVal,
	})

	l.Log(Event{
		Level:      LevelWarning,
		Category:   CategoryConfig,
		Action:     action,
		User:       user,
		RemoteAddr: remoteAddr,
		Resource:   "config",
		ResourceID: component,
		Result:     "success",
		Details:    details,
	})
}

// LogSecurity records a security event.
func (l *Logger) LogSecurity(user, remoteAddr, action, result string, reason string) {
	event := Event{
		Level:      LevelWarning,
		Category:   CategorySecurity,
		Action:     action,
		User:       user,
		RemoteAddr: remoteAddr,
		Result:     result,
	}
	if reason != "" {
		details, _ := json.Marshal(map[string]string{"reason": reason})
		event.Details = details
	}
	l.Log(event)
}

// checkRotation checks if log rotation is needed.
func (l *Logger) checkRotation() {
	if l.file == nil {
		return
	}

	info, err := l.file.Stat()
	if err != nil {
		return
	}

	// Rotate if size exceeds max
	if info.Size() > l.maxSize {
		l.rotate()
	}
}

// rotate performs log rotation.
func (l *Logger) rotate() {
	if l.file == nil {
		return
	}

	// Close current file
	l.file.Close()

	// Rename current file with timestamp
	timestamp := time.Now().Format("20060102-150405")
	backupPath := l.logPath + "." + timestamp
	_ = os.Rename(l.logPath, backupPath)

	// Open new file
	file, err := os.OpenFile(l.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		// Try to reopen old file
		file, _ = os.OpenFile(backupPath, os.O_APPEND|os.O_WRONLY, 0640)
	}
	l.file = file
	l.encoder = json.NewEncoder(file)

	// Clean up old logs
	l.cleanupOldLogs()
}

// cleanupOldLogs removes logs older than maxAge.
func (l *Logger) cleanupOldLogs() {
	dir := filepath.Dir(l.logPath)
	base := filepath.Base(l.logPath)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	cutoff := time.Now().Add(-l.maxAge)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), base+".") {
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

// IsEnabled returns true if audit logging is enabled.
func (l *Logger) IsEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.enabled
}
