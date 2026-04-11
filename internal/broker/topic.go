package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
	"github.com/chimeramq/chimera/internal/wasm"
)

// TopicMode determines how a topic's data is consumed.
type TopicMode uint8

const (
	ModeStream  TopicMode = 0
	ModeQueue   TopicMode = 1
	ModeUnified TopicMode = 2
)

// TopicConfig holds configuration for a single topic.
type TopicConfig struct {
	Name              string                  `json:"name"`
	Mode              TopicMode               `json:"mode"`
	Partitions        uint32                  `json:"partitions"`
	RetentionTime     time.Duration           `json:"retention_time"`
	RetentionSize     int64                   `json:"retention_size"`
	DLQTopic          string                  `json:"dlq_topic,omitempty"`
	MaxRetries        uint32                  `json:"max_retries"`
	DelaySupport      bool                    `json:"delay_support"`
	SchemaSubject     string                  `json:"schema_subject,omitempty"`
	SchemaEnforcement bool                    `json:"schema_enforcement"`
	PrioritySupport   bool                    `json:"priority_support"`
	DefaultTTL        int64                   `json:"default_ttl,omitempty"` // nanoseconds, 0 = no default
	TTLAction         string                  `json:"ttl_action,omitempty"`  // "drop" or "dlq"
	TransformPipeline *wasm.TransformPipeline `json:"-"`                     // WASM transform pipeline
	CreatedAt         time.Time               `json:"created_at"`
}

// TopicManager handles topic CRUD and partition routing.
type TopicManager struct {
	mu         sync.RWMutex
	topics     map[string]*TopicConfig
	metaPath   string
	storage    *hot.Engine
	wal        *wal.WAL
	rrCounters sync.Map // topic → *atomic.Uint64

	maxTopics             int
	maxPartitionsPerTopic uint32
}

// NewTopicManager loads existing topics and initializes partitions.
func NewTopicManager(dataDir string, storage *hot.Engine, w *wal.WAL, limits LimitsConfig) (*TopicManager, error) {
	tm := &TopicManager{
		topics:                make(map[string]*TopicConfig),
		metaPath:              filepath.Join(dataDir, "topics", "meta.json"),
		storage:               storage,
		wal:                   w,
		maxTopics:             limits.MaxTopics,
		maxPartitionsPerTopic: limits.MaxPartitionsPerTopic,
	}

	if err := tm.loadMetadata(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	for _, topic := range tm.topics {
		for i := uint32(0); i < topic.Partitions; i++ {
			if _, err := storage.GetOrCreatePartition(topic.Name, i); err != nil {
				return nil, fmt.Errorf("init partition %s/%d: %w", topic.Name, i, err)
			}
		}
	}

	return tm, nil
}

// CreateTopic validates and persists a new topic.
func (tm *TopicManager) CreateTopic(cfg TopicConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[cfg.Name]; exists {
		return fmt.Errorf("topic %q already exists", cfg.Name)
	}
	if tm.maxTopics > 0 && len(tm.topics) >= tm.maxTopics {
		return fmt.Errorf("maximum topic count (%d) reached", tm.maxTopics)
	}
	if err := validateTopicName(cfg.Name); err != nil {
		return err
	}
	if cfg.Partitions == 0 {
		return fmt.Errorf("partitions must be > 0")
	}
	if tm.maxPartitionsPerTopic > 0 && cfg.Partitions > tm.maxPartitionsPerTopic {
		return fmt.Errorf("partitions exceed maximum (%d > %d)", cfg.Partitions, tm.maxPartitionsPerTopic)
	}

	cfg.CreatedAt = time.Now()

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal topic config: %w", err)
	}
	if _, err := tm.wal.Append(wal.EntryTopicMeta, data); err != nil {
		return err
	}

	for i := uint32(0); i < cfg.Partitions; i++ {
		if _, err := tm.storage.GetOrCreatePartition(cfg.Name, i); err != nil {
			return err
		}
	}

	tm.topics[cfg.Name] = &cfg
	return tm.saveMetadata()
}

// DeleteTopic removes a topic from metadata.
func (tm *TopicManager) DeleteTopic(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; !exists {
		return fmt.Errorf("topic %q not found", name)
	}

	// Write tombstone to WAL so recovery doesn't resurrect the topic
	tombstone := map[string]string{"name": name, "deleted": "true"}
	data, _ := json.Marshal(tombstone)
	if _, err := tm.wal.Append(wal.EntryTopicMeta, data); err != nil {
		return fmt.Errorf("wal tombstone: %w", err)
	}

	delete(tm.topics, name)
	tm.rrCounters.Delete(name)
	return tm.saveMetadata()
}

// GetTopic returns topic config by name.
func (tm *TopicManager) GetTopic(name string) (*TopicConfig, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	cfg, ok := tm.topics[name]
	return cfg, ok
}

// ListTopics returns all topic configs.
func (tm *TopicManager) ListTopics() []*TopicConfig {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	result := make([]*TopicConfig, 0, len(tm.topics))
	for _, cfg := range tm.topics {
		result = append(result, cfg)
	}
	return result
}

// ResolvePartition determines which partition a message goes to.
func (tm *TopicManager) ResolvePartition(topic string, routingKey string, partitionCount uint32) uint32 {
	if routingKey == "" {
		return tm.roundRobinPartition(topic, partitionCount)
	}
	return murmur3Hash([]byte(routingKey)) % partitionCount
}

func (tm *TopicManager) roundRobinPartition(topic string, count uint32) uint32 {
	val, _ := tm.rrCounters.LoadOrStore(topic, &atomic.Uint64{})
	counter := val.(*atomic.Uint64)
	return uint32(counter.Add(1)-1) % count
}

func (tm *TopicManager) loadMetadata() error {
	data, err := os.ReadFile(tm.metaPath)
	if err != nil {
		return err
	}
	var topics []*TopicConfig
	if err := json.Unmarshal(data, &topics); err != nil {
		return err
	}
	for _, t := range topics {
		tm.topics[t.Name] = t
	}
	return nil
}

func (tm *TopicManager) saveMetadata() error {
	topics := make([]*TopicConfig, 0, len(tm.topics))
	for _, t := range tm.topics {
		topics = append(topics, t)
	}
	data, err := json.MarshalIndent(topics, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(tm.metaPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("create topics dir: %w", err)
	}

	tmpPath := tm.metaPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0640); err != nil {
		return err
	}
	return os.Rename(tmpPath, tm.metaPath)
}

func validateTopicName(name string) error {
	if len(name) == 0 || len(name) > 255 {
		return fmt.Errorf("topic name must be 1-255 characters")
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_') {
			return fmt.Errorf("topic name contains invalid character: %c", c)
		}
	}
	if name[0] == '.' || name[0] == '-' {
		return fmt.Errorf("topic name cannot start with '.' or '-'")
	}
	return nil
}
