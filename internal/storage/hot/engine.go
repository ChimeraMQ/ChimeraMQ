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
// Uses read-lock for fast path (partition exists) and write-lock only for creation.
func (e *Engine) GetOrCreatePartition(topic string, partID uint32) (*Partition, error) {
	// Fast path: read lock
	e.mu.RLock()
	topicParts, ok := e.partitions[topic]
	if ok {
		part, ok := topicParts[partID]
		if ok {
			e.mu.RUnlock()
			return part, nil
		}
	}
	e.mu.RUnlock()

	// Slow path: write lock
	e.mu.Lock()
	defer e.mu.Unlock()

	// Double-check after acquiring write lock
	topicParts, ok = e.partitions[topic]
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

// ForEachPartition calls fn for each partition. Stops if fn returns false.
func (e *Engine) ForEachPartition(fn func(topic string, partID uint32, p *Partition) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for topic, topicParts := range e.partitions {
		for partID, part := range topicParts {
			if !fn(topic, partID, part) {
				return
			}
		}
	}
}

// TotalSize returns the total size of all partitions in bytes.
func (e *Engine) TotalSize() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var total int64
	for _, topicParts := range e.partitions {
		for _, part := range topicParts {
			total += part.TotalSize()
		}
	}
	return total
}
