package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SnapshotMeta holds metadata about a snapshot.
type SnapshotMeta struct {
	LastIncludedIndex Index `json:"last_included_index"`
	LastIncludedTerm  Term  `json:"last_included_term"`
	Size              int   `json:"size"`
}

// Snapshotter manages snapshot persistence.
type Snapshotter struct {
	mu  sync.Mutex
	dir string
	fsm *MetadataFSM
	log *RaftLog
}

// NewSnapshotter creates a new snapshot manager.
func NewSnapshotter(dir string, fsm *MetadataFSM, log *RaftLog) *Snapshotter {
	return &Snapshotter{
		dir: dir,
		fsm: fsm,
		log: log,
	}
}

// TakeSnapshot creates a new snapshot and compacts the log.
func (s *Snapshotter) TakeSnapshot() (*SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return nil, fmt.Errorf("create snapshot dir: %w", err)
	}

	// Get FSM state
	data, err := s.fsm.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("fsm snapshot: %w", err)
	}

	lastIndex := s.log.LastIndex()
	lastTerm := s.log.LastTerm()

	meta := &SnapshotMeta{
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Size:              len(data),
	}

	// Write snapshot data
	snapPath := filepath.Join(s.dir, "snapshot.json")
	tmp := snapPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return nil, fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, snapPath); err != nil {
		return nil, fmt.Errorf("rename snapshot: %w", err)
	}

	// Write metadata
	metaPath := filepath.Join(s.dir, "meta.json")
	metaData, _ := json.Marshal(meta)
	metaTmp := metaPath + ".tmp"
	if err := os.WriteFile(metaTmp, metaData, 0644); err != nil {
		return nil, fmt.Errorf("write meta: %w", err)
	}
	if err := os.Rename(metaTmp, metaPath); err != nil {
		return nil, fmt.Errorf("rename meta: %w", err)
	}

	// Compact log
	if lastIndex > 0 {
		s.log.Compact(lastIndex)
		s.log.Save()
	}

	return meta, nil
}

// LoadSnapshot restores the latest snapshot.
func (s *Snapshotter) LoadSnapshot() (*SnapshotMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	metaPath := filepath.Join(s.dir, "meta.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var meta SnapshotMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, err
	}

	snapPath := filepath.Join(s.dir, "snapshot.json")
	data, err := os.ReadFile(snapPath)
	if err != nil {
		return nil, err
	}

	if err := s.fsm.Restore(data); err != nil {
		return nil, fmt.Errorf("restore fsm: %w", err)
	}

	return &meta, nil
}

// ShouldSnapshot returns true if the log has grown beyond the threshold.
func (s *Snapshotter) ShouldSnapshot(maxEntries int) bool {
	return s.log.Len() > maxEntries
}
