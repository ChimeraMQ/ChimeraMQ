package dlq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// DLQ handles dead letter queue operations for failed messages.
type DLQ struct {
	mu          sync.RWMutex
	topicPrefix string
	maxRetries  int
	queues      map[string]*deadQueue // topic → dead letter queue
	enabled     atomic.Bool
	dataDir     string
}

type deadQueue struct {
	messages []*DLQEntry
	offset   uint64
}

// DLQEntry represents a message in the dead letter queue.
type DLQEntry struct {
	ID          uint64            `json:"id"`
	OriginalMsg *message.Envelope `json:"original_msg"`
	Topic       string            `json:"topic"`
	Partition   uint32            `json:"partition"`
	Reason      string            `json:"reason"`
	Retries     int               `json:"retries"`
	FailedAt    time.Time         `json:"failed_at"`
}

// Config holds DLQ configuration.
type Config struct {
	Enabled     bool
	TopicPrefix string // default: "__dlq_"
	MaxRetries  int    // default: 3
	DataDir     string // directory for persistent storage
}

// NewDLQ creates a new dead letter queue handler.
func NewDLQ(cfg Config) *DLQ {
	prefix := cfg.TopicPrefix
	if prefix == "" {
		prefix = "__dlq_"
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	d := &DLQ{
		topicPrefix: prefix,
		maxRetries:  maxRetries,
		queues:      make(map[string]*deadQueue),
		dataDir:     cfg.DataDir,
	}
	if cfg.Enabled {
		d.enabled.Store(true)
	}
	if d.dataDir != "" {
		_ = os.MkdirAll(d.dataDir, 0750)
		d.loadAll()
	}
	return d
}

// Enabled returns whether DLQ is active.
func (d *DLQ) Enabled() bool { return d.enabled.Load() }

// MaxRetries returns the configured max retries.
func (d *DLQ) MaxRetries() int { return d.maxRetries }

// DLQTopic returns the DLQ topic name for a source topic.
func (d *DLQ) DLQTopic(topic string) string {
	return d.topicPrefix + topic
}

// Push adds a failed message to the dead letter queue.
func (d *DLQ) Push(msg *message.Envelope, topic string, partition uint32, reason string, retryCount int) {
	if !d.enabled.Load() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	q, ok := d.queues[topic]
	if !ok {
		q = &deadQueue{}
		d.queues[topic] = q
	}
	q.offset++
	entry := &DLQEntry{
		ID:          q.offset,
		OriginalMsg: msg,
		Topic:       topic,
		Partition:   partition,
		Reason:      reason,
		Retries:     retryCount,
		FailedAt:    time.Now(),
	}
	q.messages = append(q.messages, entry)
	d.persistEntry(entry)
}

// ShouldDLQ checks if a message should be sent to the DLQ based on retry count.
func (d *DLQ) ShouldDLQ(retryCount int) bool {
	return d.enabled.Load() && retryCount >= d.maxRetries
}

// Peek returns entries from a topic's DLQ without removing them.
func (d *DLQ) Peek(topic string, limit int) []*DLQEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()

	q, ok := d.queues[topic]
	if !ok {
		return nil
	}
	n := len(q.messages)
	if limit > 0 && limit < n {
		n = limit
	}
	result := make([]*DLQEntry, n)
	copy(result, q.messages[:n])
	return result
}

// Pop removes and returns the first entry from a topic's DLQ.
func (d *DLQ) Pop(topic string) *DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()

	q, ok := d.queues[topic]
	if !ok || len(q.messages) == 0 {
		return nil
	}
	entry := q.messages[0]
	q.messages = q.messages[1:]
	return entry
}

// Size returns the number of entries in a topic's DLQ.
func (d *DLQ) Size(topic string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	q, ok := d.queues[topic]
	if !ok {
		return 0
	}
	return len(q.messages)
}

// TotalSize returns total entries across all DLQs.
func (d *DLQ) TotalSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	total := 0
	for _, q := range d.queues {
		total += len(q.messages)
	}
	return total
}

// Topics returns all topics that have DLQ entries.
func (d *DLQ) Topics() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	topics := make([]string, 0, len(d.queues))
	for t := range d.queues {
		topics = append(topics, t)
	}
	return topics
}

// Clear removes all entries from a topic's DLQ.
func (d *DLQ) Clear(topic string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.queues, topic)
	if d.dataDir != "" {
		_ = os.Remove(d.topicPath(topic))
	}
}

// String implements fmt.Stringer.
func (e *DLQEntry) String() string {
	return fmt.Sprintf("DLQEntry{ID:%d, Topic:%s, Partition:%d, Reason:%q, Retries:%d, FailedAt:%s}",
		e.ID, e.Topic, e.Partition, e.Reason, e.Retries, e.FailedAt.Format(time.RFC3339))
}

// persistEntry appends a DLQ entry to the topic's file.
func (d *DLQ) persistEntry(entry *DLQEntry) {
	if d.dataDir == "" {
		return
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	path := d.topicPath(entry.Topic)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(data)
	_, _ = f.Write([]byte("\n"))
}

// loadAll loads persisted DLQ entries from disk.
func (d *DLQ) loadAll() {
	entries, err := os.ReadDir(d.dataDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		topic := e.Name()[:len(e.Name())-6] // strip .jsonl
		d.loadTopic(topic)
	}
}

// loadTopic loads entries for a single topic from disk.
func (d *DLQ) loadTopic(topic string) {
	path := d.topicPath(topic)
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	q := &deadQueue{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry DLQEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if entry.ID > q.offset {
			q.offset = entry.ID
		}
		q.messages = append(q.messages, &entry)
	}
	if len(q.messages) > 0 {
		d.queues[topic] = q
	}
}

// topicPath returns the file path for a topic's DLQ file.
func (d *DLQ) topicPath(topic string) string {
	return filepath.Join(d.dataDir, topic+".jsonl")
}
