package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// RaftLog manages the replicated log.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	// dir is the persistence directory.
	dir string
	// firstIndex is the index of the first entry (after compaction).
	firstIndex Index
}

// NewRaftLog creates a new Raft log.
func NewRaftLog(dir string) *RaftLog {
	return &RaftLog{
		entries:    make([]LogEntry, 0),
		dir:        dir,
		firstIndex: 1,
	}
}

// Load restores the log from disk.
// Tries binary format first, falls back to legacy JSON for migration.
func (l *RaftLog) Load() error {
	// Try binary format first
	if err := l.loadLogBinary(); err == nil {
		return nil
	}

	// Fall back to legacy JSON format for migration
	l.mu.Lock()
	defer l.mu.Unlock()

	path := filepath.Join(l.dir, "log.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}

	var stored struct {
		Entries    []LogEntry `json:"entries"`
		FirstIndex Index      `json:"first_index"`
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}
	l.entries = stored.Entries
	l.firstIndex = stored.FirstIndex
	return nil
}

// Save persists the log to disk using binary format.
func (l *RaftLog) Save() error {
	return l.saveLogBinary()
}

// Append adds entries to the log.
func (l *RaftLog) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// LastIndex returns the index of the last entry, or 0 if empty.
func (l *RaftLog) LastIndex() Index {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

func (l *RaftLog) lastIndexLocked() Index {
	if len(l.entries) == 0 {
		return l.firstIndex - 1
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry.
func (l *RaftLog) LastTerm() Term {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// Get returns the entry at the given index, or nil if not found.
func (l *RaftLog) Get(idx Index) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getLocked(idx)
}

func (l *RaftLog) getLocked(idx Index) *LogEntry {
	if idx < l.firstIndex {
		return nil
	}
	offset := int(idx - l.firstIndex)
	if offset >= len(l.entries) {
		return nil
	}
	return &l.entries[offset]
}

// Range returns entries [from, to) (from-inclusive, to-exclusive).
func (l *RaftLog) Range(from, to Index) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.rangeLocked(from, to)
}

func (l *RaftLog) rangeLocked(from, to Index) []LogEntry {
	if from < l.firstIndex {
		from = l.firstIndex
	}
	start := int(from - l.firstIndex)
	end := int(to - l.firstIndex)
	if start < 0 {
		start = 0
	}
	if end > len(l.entries) {
		end = len(l.entries)
	}
	if start >= end {
		return nil
	}
	result := make([]LogEntry, end-start)
	copy(result, l.entries[start:end])
	return result
}

// TruncateAfter removes all entries with index > idx.
func (l *RaftLog) TruncateAfter(idx Index) {
	l.mu.Lock()
	defer l.mu.Unlock()
	end := int(idx - l.firstIndex + 1)
	if end > len(l.entries) {
		return
	}
	if end < 0 {
		l.entries = l.entries[:0]
		return
	}
	l.entries = l.entries[:end]
}

// Compact removes all entries up to (but not including) throughIndex.
func (l *RaftLog) Compact(throughIndex Index) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if throughIndex < l.firstIndex {
		return 0
	}
	cut := int(throughIndex - l.firstIndex + 1)
	if cut >= len(l.entries) {
		return 0
	}
	removed := cut
	l.entries = l.entries[cut:]
	l.firstIndex = throughIndex + 1
	return removed
}

// Len returns the number of entries in the log.
func (l *RaftLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// TermAt returns the term of the entry at idx.
func (l *RaftLog) TermAt(idx Index) Term {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e := l.getLocked(idx)
	if e == nil {
		return 0
	}
	return e.Term
}
