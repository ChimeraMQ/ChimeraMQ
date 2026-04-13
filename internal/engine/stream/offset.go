package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

// validGroupName validates consumer group names to prevent path traversal.
// Group names must contain only alphanumeric characters, hyphens, and underscores.
var validGroupName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// ValidateGroupName returns an error if the group name contains invalid characters.
func ValidateGroupName(group string) error {
	if group == "" {
		return fmt.Errorf("group name cannot be empty")
	}
	if len(group) > 256 {
		return fmt.Errorf("group name too long (max 256 characters)")
	}
	if !validGroupName.MatchString(group) {
		return fmt.Errorf("invalid group name %q: must contain only alphanumeric characters, hyphens, and underscores", group)
	}
	return nil
}

// OffsetStore persists consumer group offsets to disk.
type OffsetStore struct {
	mu    sync.RWMutex
	dir   string
	cache map[string]map[uint32]uint64 // group → partition → offset
}

// NewOffsetStore creates a new offset store.
func NewOffsetStore(dataDir string) (*OffsetStore, error) {
	dir := filepath.Join(dataDir, "consumers")
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create consumers dir: %w", err)
	}

	s := &OffsetStore{
		dir:   dir,
		cache: make(map[string]map[uint32]uint64),
	}
	s.loadAll()
	return s, nil
}

// Save persists an offset for a consumer group and partition.
func (s *OffsetStore) Save(group string, partitionID uint32, offset uint64) error {
	if err := ValidateGroupName(group); err != nil {
		return err
	}

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
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return fmt.Errorf("create group dir: %w", err)
	}
	data, err := json.Marshal(s.cache[group])
	if err != nil {
		return fmt.Errorf("marshal offsets: %w", err)
	}
	// Atomic write: write to temp file then rename
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0640); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
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
		group := e.Name()
		// Skip directories with invalid names (path traversal protection)
		if err := ValidateGroupName(group); err != nil {
			continue
		}
		path := filepath.Join(s.dir, group, "offsets.json")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		offsets := make(map[uint32]uint64)
		if err := json.Unmarshal(data, &offsets); err != nil {
			continue
		}
		s.cache[group] = offsets
	}
}
