package warm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Manifest tracks all SSTables and their level assignments.
type Manifest struct {
	mu      sync.Mutex
	path    string
	entries []ManifestEntry
}

// ManifestEntry records an SSTable's position in the LSM tree.
type ManifestEntry struct {
	Level    int    `json:"level"`
	SSTPath  string `json:"sst_path"`
	MinOff   uint64 `json:"min_off"`
	MaxOff   uint64 `json:"max_off"`
	EntryCount uint32 `json:"entry_count"`
}

// NewManifest creates or loads a manifest.
func NewManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, "manifest.json")
	m := &Manifest{path: path}

	data, err := os.ReadFile(path)
	if err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &m.entries); err != nil {
			return nil, err
		}
	}

	return m, nil
}

// Add adds an SSTable to the manifest.
func (m *Manifest) Add(level int, sst *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta := sst.Metadata()
	m.entries = append(m.entries, ManifestEntry{
		Level:      level,
		SSTPath:    sst.Path(),
		MinOff:     meta.MinOffset,
		MaxOff:     meta.MaxOffset,
		EntryCount: meta.EntryCount,
	})
	m.save()
}

// Remove removes an SSTable from the manifest.
func (m *Manifest) Remove(sstPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filtered := make([]ManifestEntry, 0, len(m.entries))
	for _, e := range m.entries {
		if e.SSTPath != sstPath {
			filtered = append(filtered, e)
		}
	}
	m.entries = filtered
	m.save()
}

// EntriesAt returns all manifest entries at the given level.
func (m *Manifest) EntriesAt(level int) []ManifestEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []ManifestEntry
	for _, e := range m.entries {
		if e.Level == level {
			result = append(result, e)
		}
	}
	return result
}

// AllEntries returns all manifest entries.
func (m *Manifest) AllEntries() []ManifestEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]ManifestEntry, len(m.entries))
	copy(result, m.entries)
	return result
}

// SSTCount returns the number of SSTables at a given level.
func (m *Manifest) SSTCount(level int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, e := range m.entries {
		if e.Level == level {
			count++
		}
	}
	return count
}

func (m *Manifest) save() {
	data, _ := json.Marshal(m.entries)
	tmp := m.path + ".tmp"
	os.WriteFile(tmp, data, 0644)
	os.Rename(tmp, m.path)
}
