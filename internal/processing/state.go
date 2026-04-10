package processing

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/chimeramq/chimera/internal/storage/warm"
)

// StateStore provides persistent key-value state for stream processors.
type StateStore struct {
	mu    sync.RWMutex
	name  string
	lsm   *warm.LSMTree
	cache map[string][]byte // in-memory overlay
}

// NewStateStore creates a new state store backed by an LSM-Tree.
func NewStateStore(name, dir string) (*StateStore, error) {
	storeDir := filepath.Join(dir, name)
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir %q: %w", storeDir, err)
	}

	lsm, err := warm.NewLSMTree(storeDir, warm.DefaultLSMConfig())
	if err != nil {
		return nil, fmt.Errorf("open LSM for state store %q: %w", name, err)
	}

	return &StateStore{
		name:  name,
		lsm:   lsm,
		cache: make(map[string][]byte),
	}, nil
}

// Get retrieves a value from the state store.
// Returns (value, ok, deleted) where ok=true if the key exists.
func (s *StateStore) Get(key []byte) ([]byte, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache first
	if val, ok := s.cache[string(key)]; ok {
		if val == nil {
			return nil, false, true // deleted in cache
		}
		return val, true, false
	}

	return s.lsm.Get(key)
}

// Put stores a key-value pair in the state store.
func (s *StateStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache[string(key)] = value
	return nil
}

// Delete removes a key from the state store.
func (s *StateStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache[string(key)] = nil
	return nil
}

// Flush persists the in-memory cache to the LSM-Tree.
func (s *StateStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range s.cache {
		if v == nil {
			if err := s.lsm.Delete([]byte(k)); err != nil {
				return err
			}
		} else {
			if err := s.lsm.Put([]byte(k), v); err != nil {
				return err
			}
		}
	}

	s.cache = make(map[string][]byte)
	return nil
}

// Close flushes and closes the state store.
func (s *StateStore) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	return s.lsm.Close()
}

// Name returns the store name.
func (s *StateStore) Name() string { return s.name }
