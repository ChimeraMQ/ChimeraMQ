package hot

import (
	"path/filepath"
	"sync"
)

// Engine manages all partitions across all topics.
type Engine struct {
	mu         sync.RWMutex
	baseDir    string
	partitions map[string]map[uint32]*Partition // topic → partitionID → Partition
	config     HotConfig
}

// HotConfig holds hot tier configuration.
type HotConfig struct {
	SegmentSize int64
}

// NewEngine creates a new storage engine.
func NewEngine(baseDir string, cfg HotConfig) *Engine {
	return &Engine{
		baseDir:    baseDir,
		partitions: make(map[string]map[uint32]*Partition),
		config:     cfg,
	}
}

// GetOrCreatePartition lazily creates or returns an existing partition.
func (e *Engine) GetOrCreatePartition(topic string, partID uint32) (*Partition, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	topicParts, ok := e.partitions[topic]
	if !ok {
		topicParts = make(map[uint32]*Partition)
		e.partitions[topic] = topicParts
	}

	part, ok := topicParts[partID]
	if !ok {
		dir := filepath.Join(e.baseDir, "topics", topic)
		var err error
		part, err = OpenPartition(dir, topic, partID, e.config.SegmentSize)
		if err != nil {
			return nil, err
		}
		topicParts[partID] = part
	}

	return part, nil
}

// Close closes all partitions.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, topicParts := range e.partitions {
		for _, part := range topicParts {
			part.Close()
		}
	}
	return nil
}

// FlushAll syncs all active segments to disk.
func (e *Engine) FlushAll() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, topicParts := range e.partitions {
		for _, part := range topicParts {
			part.mu.RLock()
			if part.active != nil && part.active.file != nil {
				_ = part.active.file.Sync()
			}
			part.mu.RUnlock()
		}
	}
}
