package warm

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// LSMConfig holds configuration for the LSM-Tree.
type LSMConfig struct {
	MemTableCapacity   int64
	BlockSize          int
	BloomFPRate        float64
	CompactionStrategy string // size_tiered, leveled, tombstone
	CompactionInterval time.Duration
	MaxLevel           int
	LevelSizeRatio     int // size ratio between levels (default 10)
	MaxSSTables        int // maximum total SSTables before blocking writes (default 100)
}

// DefaultLSMConfig returns sensible defaults.
func DefaultLSMConfig() LSMConfig {
	return LSMConfig{
		MemTableCapacity:   4 * 1024 * 1024,
		BlockSize:          blockSizeDefault,
		BloomFPRate:        0.01,
		CompactionStrategy: "size_tiered",
		CompactionInterval: 5 * time.Minute,
		MaxLevel:           7,
		LevelSizeRatio:     10,
		MaxSSTables:        100,
	}
}

// LSMStats holds statistics about the LSM-Tree.
type LSMStats struct {
	MemTableSize   int64
	MemTableCount  uint32
	ImmutableCount int
	TotalSSTables  int
	Levels         []LevelStats
}

// LevelStats holds per-level statistics.
type LevelStats struct {
	Level     int
	SSTables  int
	TotalSize int64
}

// LSMTree manages a multi-level sorted merge tree.
type LSMTree struct {
	mu         sync.RWMutex
	dir        string
	config     LSMConfig
	memtable   *MemTable
	immutables []*MemTable
	levels     []Level
	manifest   *Manifest
	flushing   atomic.Bool
	closed     bool

	flushCh chan struct{}
	stopCh  chan struct{}
	done    chan struct{}
}

// Level holds SSTables at a given level.
type Level struct {
	id       int
	sstables []*SSTable
}

// NewLSMTree creates a new LSM-Tree.
func NewLSMTree(dir string, config LSMConfig) (*LSMTree, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create LSM dir: %w", err)
	}

	// Ensure reasonable defaults
	if config.MaxSSTables <= 0 {
		config.MaxSSTables = 100
	}

	manifest, err := NewManifest(dir)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	levels := make([]Level, config.MaxLevel)
	for i := range levels {
		levels[i] = Level{id: i}
	}

	// Recover SSTables from manifest
	for _, entry := range manifest.AllEntries() {
		if entry.Level >= len(levels) {
			continue
		}
		sst, err := OpenSSTable(entry.SSTPath)
		if err != nil {
			continue // skip corrupt SSTables
		}
		levels[entry.Level].sstables = append(levels[entry.Level].sstables, sst)
	}

	lsm := &LSMTree{
		dir:      dir,
		config:   config,
		memtable: NewMemTable(config.MemTableCapacity),
		levels:   levels,
		manifest: manifest,
		flushCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}

	go lsm.run()
	return lsm, nil
}

// Put inserts a key-value pair.
func (lsm *LSMTree) Put(key, value []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if lsm.closed {
		return errLSMClosed
	}

	if err := lsm.memtable.Put(key, value); err != nil {
		return err
	}

	// Check if memtable needs to be flushed
	if lsm.memtable.AtCapacity() {
		lsm.immutables = append(lsm.immutables, lsm.memtable)
		lsm.memtable = NewMemTable(lsm.config.MemTableCapacity)
		select {
		case lsm.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// Delete inserts a tombstone.
func (lsm *LSMTree) Delete(key []byte) error {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if lsm.closed {
		return errLSMClosed
	}

	return lsm.memtable.Delete(key)
}

// Get looks up a key. Returns (value, found, deleted).
func (lsm *LSMTree) Get(key []byte) ([]byte, bool, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	// Check active memtable
	if val, found, deleted := lsm.memtable.Get(key); found {
		return val, true, deleted
	}

	// Check immutable memtables (newest first)
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if val, found, deleted := lsm.immutables[i].Get(key); found {
			return val, true, deleted
		}
	}

	// Check levels (L0 first, then L1+)
	for _, level := range lsm.levels {
		// L0: check all SSTables (overlapping ranges)
		// L1+: could binary search, but for now check all
		for i := len(level.sstables) - 1; i >= 0; i-- {
			if val, found, deleted := level.sstables[i].Get(key); found {
				return val, true, deleted
			}
		}
	}

	return nil, false, false
}

// Close shuts down the LSM-Tree.
func (lsm *LSMTree) Close() error {
	lsm.mu.Lock()
	lsm.closed = true
	lsm.mu.Unlock()

	close(lsm.stopCh)
	<-lsm.done

	// Close all SSTables
	for _, level := range lsm.levels {
		for _, sst := range level.sstables {
			sst.Close()
		}
	}

	return nil
}

// Stats returns current LSM statistics.
func (lsm *LSMTree) Stats() LSMStats {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	stats := LSMStats{
		MemTableSize:   lsm.memtable.Size(),
		MemTableCount:  lsm.memtable.Count(),
		ImmutableCount: len(lsm.immutables),
	}

	for _, level := range lsm.levels {
		levelStat := LevelStats{
			Level:    level.id,
			SSTables: len(level.sstables),
		}
		stats.TotalSSTables += len(level.sstables)
		stats.Levels = append(stats.Levels, levelStat)
	}

	return stats
}

func (lsm *LSMTree) run() {
	defer close(lsm.done)

	for {
		select {
		case <-lsm.stopCh:
			return
		case <-lsm.flushCh:
			lsm.flushImmutable()
		}
	}
}

func (lsm *LSMTree) flushImmutable() {
	if !lsm.flushing.CompareAndSwap(false, true) {
		return
	}
	defer lsm.flushing.Store(false)

	lsm.mu.Lock()
	if len(lsm.immutables) == 0 {
		lsm.mu.Unlock()
		return
	}

	// Check if we've reached the maximum SSTable limit
	totalSSTables := 0
	for _, level := range lsm.levels {
		totalSSTables += len(level.sstables)
	}
	if totalSSTables >= lsm.config.MaxSSTables {
		lsm.mu.Unlock()
		slog.Warn("LSMTree: max SSTables reached, delaying flush", "max", lsm.config.MaxSSTables)
		return
	}

	mt := lsm.immutables[0]
	lsm.immutables = lsm.immutables[1:]
	lsm.mu.Unlock()

	mt.Freeze()

	sst, err := FlushMemTable(mt, lsm.dir)
	if err != nil {
		return
	}

	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	if lsm.closed {
		sst.Close()
		return
	}

	lsm.levels[0].sstables = append(lsm.levels[0].sstables, sst)
	if err := lsm.manifest.Add(0, sst); err != nil {
		slog.Error("manifest add L0", "err", err)
	}

	// Trigger compaction if L0 has too many SSTables
	if len(lsm.levels[0].sstables) >= 4 {
		go lsm.compactL0()
	}
}

func (lsm *LSMTree) compactL0() {
	lsm.mu.Lock()
	if len(lsm.levels[0].sstables) < 4 {
		lsm.mu.Unlock()
		return
	}

	// Collect L0 SSTables for compaction
	toCompact := lsm.levels[0].sstables
	lsm.levels[0].sstables = nil
	lsm.mu.Unlock()

	// Merge all entries from SSTables
	merged := make(map[uint64][]byte) // offset -> value
	deleted := make(map[uint64]bool)  // offset -> deleted

	for _, sst := range toCompact {
		entries := collectEntries(sst)
		for off, entry := range entries {
			merged[off] = entry.Value
			deleted[off] = entry.Deleted
		}
	}

	// Create new MemTable from merged data
	mt := NewMemTable(int64(len(merged)) * 256) // rough estimate
	for off, val := range merged {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, off)
		if deleted[off] {
			_ = mt.Delete(key)
		} else {
			_ = mt.Put(key, val)
		}
	}
	mt.Freeze()

	newSST, err := FlushMemTable(mt, lsm.dir)
	if err != nil {
		// Put back the old SSTables
		lsm.mu.Lock()
		lsm.levels[0].sstables = append(toCompact, lsm.levels[0].sstables...)
		lsm.mu.Unlock()
		return
	}

	// Remove old SSTables
	for _, sst := range toCompact {
		if err := lsm.manifest.Remove(sst.Path()); err != nil {
			slog.Error("manifest remove", "err", err)
		}
		if err := sst.Remove(); err != nil {
			slog.Error("sst remove", "path", sst.Path(), "err", err)
		}
	}

	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	lsm.levels[1].sstables = append(lsm.levels[1].sstables, newSST)
	if err := lsm.manifest.Add(1, newSST); err != nil {
		slog.Error("manifest add L1", "err", err)
	}
}

// OldSSTables returns SSTables at L1+ older than the given threshold.
func (lsm *LSMTree) OldSSTables(olderThan time.Duration) []*SSTable {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	cutoff := time.Now().Add(-olderThan)
	var result []*SSTable
	for i := 1; i < len(lsm.levels); i++ {
		for _, sst := range lsm.levels[i].sstables {
			meta := sst.Metadata()
			if time.Unix(0, meta.MaxTimestamp).Before(cutoff) {
				result = append(result, sst)
			}
		}
	}
	return result
}

// RemoveSSTable removes an SSTable from its level and the manifest.
func (lsm *LSMTree) RemoveSSTable(sst *SSTable) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

	for i := range lsm.levels {
		for j, s := range lsm.levels[i].sstables {
			if s == sst {
				lsm.levels[i].sstables = append(lsm.levels[i].sstables[:j], lsm.levels[i].sstables[j+1:]...)
				break
			}
		}
	}
	if err := lsm.manifest.Remove(sst.Path()); err != nil {
		slog.Error("manifest remove", "err", err)
	}
	if err := sst.Remove(); err != nil {
		slog.Error("sst remove", "path", sst.Path(), "err", err)
	}
}

// TotalSize returns the total size of all SSTables in bytes.
func (lsm *LSMTree) TotalSize() int64 {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	var total int64
	for _, level := range lsm.levels {
		for _, sst := range level.sstables {
			total += sst.Size()
		}
	}
	return total
}

func collectEntries(sst *SSTable) map[uint64]MemTableEntry {
	result := make(map[uint64]MemTableEntry)
	meta := sst.Metadata()
	for off := meta.MinOffset; off <= meta.MaxOffset; off++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, off)
		val, found, deleted := sst.Get(key)
		if found {
			result[off] = MemTableEntry{Value: val, Deleted: deleted}
		}
	}
	return result
}
