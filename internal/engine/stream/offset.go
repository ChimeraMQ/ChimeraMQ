package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OffsetStore persists consumer group offsets to disk.
type OffsetStore struct {
	mu    sync.RWMutex
	dir   string
	cache map[string]map[uint32]uint64 // group → partition → offset
}

// NewOffsetStore creates a new offset store.
func NewOffsetStore(dataDir string) *OffsetStore {
	dir := filepath.Join(dataDir, "consumers")
	os.MkdirAll(dir, 0750)

	s := &OffsetStore{
		dir:   dir,
		cache: make(map[string]map[uint32]uint64),
	}
	s.loadAll()
	return s
}

// Save persists an offset for a consumer group and partition.
func (s *OffsetStore) Save(group string, partitionID uint32, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cache[group] == nil {
		s.cache[group] = make(map[uint32]uint64)
	}
	s.cache[group][partitionID] = offset
	return s.persist(group)
}

// Get returns the committed offset for a group and partition.
func (s *OffsetStore) Get(group string, partitionID uint32) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if g, ok := s.cache[group]; ok {
		return g[partitionID]
	}
	return 0
}

func (s *OffsetStore) persist(group string) error {
	path := filepath.Join(s.dir, group, "offsets.json")
	os.MkdirAll(filepath.Dir(path), 0750)
	data, err := json.Marshal(s.cache[group])
	if err != nil {
		return fmt.Errorf("marshal offsets: %w", err)
	}
	return os.WriteFile(path, data, 0640)
}

func (s *OffsetStore) loadAll() {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(s.dir, e.Name(), "offsets.json")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		offsets := make(map[uint32]uint64)
		if err := json.Unmarshal(data, &offsets); err != nil {
			continue
		}
		s.cache[e.Name()] = offsets
	}
}
