package hot

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/chimeramq/chimera/internal/message"
)

// CompactionMode controls how log compaction works.
type CompactionMode int

const (
	CompactNone     CompactionMode = iota
	CompactKeyBased                // Keep latest value per key
)

// LogCompactor performs log compaction on a partition.
type LogCompactor struct {
	mu       sync.Mutex
	mode     CompactionMode
	enabled  bool
}

// NewLogCompactor creates a new compactor.
func NewLogCompactor(mode CompactionMode) *LogCompactor {
	return &LogCompactor{
		mode:    mode,
		enabled: mode != CompactNone,
	}
}

// Enabled returns whether compaction is active.
func (lc *LogCompactor) Enabled() bool { return lc.enabled }

// ShouldCompact returns true if the partition has enough frozen segments to justify compaction.
func (lc *LogCompactor) ShouldCompact(p *Partition) bool {
	if !lc.enabled {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	frozen := 0
	for _, seg := range p.segments {
		if seg.frozen {
			frozen++
		}
	}
	return frozen >= 2
}

// Compact performs key-based log compaction on a partition.
// It reads all frozen segments, keeps only the latest message per routing key,
// and writes a new compacted segment.
func (lc *LogCompactor) Compact(p *Partition) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Identify frozen segments (exclude active)
	var frozen []*Segment
	for _, seg := range p.segments {
		if seg.frozen {
			frozen = append(frozen, seg)
		}
	}
	if len(frozen) < 2 {
		return nil // nothing to compact
	}

	// Read all messages and keep latest per key
	latest := make(map[string][]byte) // routingKey → raw data
	var keyless [][]byte              // messages without routing key
	totalRead := 0

	for _, seg := range frozen {
		records, err := lc.readAllRecords(seg)
		if err != nil {
			continue
		}
		for _, data := range records {
			totalRead++
			env, err := message.Unmarshal(data)
			if err != nil {
				continue
			}
			if env.RoutingKey == "" {
				keyless = append(keyless, data)
			} else {
				latest[env.RoutingKey] = data
			}
		}
	}

	if totalRead == 0 {
		return nil
	}

	// Write compacted segment
	compactedPath := filepath.Join(p.dir, fmt.Sprintf("%020d.compacted.log", frozen[0].BaseOffset()))
	compactedFile, err := os.Create(compactedPath)
	if err != nil {
		return fmt.Errorf("create compacted segment: %w", err)
	}

	written := 0
	writeRecord := func(data []byte) error {
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err := compactedFile.Write(lenBuf); err != nil {
			return err
		}
		if _, err := compactedFile.Write(data); err != nil {
			return err
		}
		written++
		return nil
	}

	// Write keyless first
	for _, data := range keyless {
		if err := writeRecord(data); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return err
		}
	}
	// Write latest per key
	for _, data := range latest {
		if err := writeRecord(data); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return err
		}
	}
	compactedFile.Sync()
	compactedFile.Close()

	// Replace old segments with compacted one
	var newSegments []*Segment
	for _, seg := range p.segments {
		if seg.frozen {
			seg.Close()
			os.Remove(seg.file.Name())
			// Remove associated index file
			indexPath := seg.file.Name()[:len(seg.file.Name())-4] + ".idx"
			os.Remove(indexPath)
		} else {
			newSegments = append(newSegments, seg)
		}
	}

	// Open compacted segment and insert before active
	compactedSeg, err := OpenSegment(compactedPath, frozen[0].BaseOffset(), p.maxSegSize)
	if err != nil {
		return fmt.Errorf("open compacted segment: %w", err)
	}
	compactedSeg.frozen = true

	// Rebuild segment list: compacted + remaining (active)
	rebuilt := []*Segment{compactedSeg}
	for _, seg := range newSegments {
		rebuilt = append(rebuilt, seg)
	}
	p.segments = rebuilt

	// Update log start offset
	if len(p.segments) > 0 {
		p.logStart = p.segments[0].BaseOffset()
	}

	return nil
}

// CompactionStats returns statistics about compaction.
type CompactionStats struct {
	FrozenSegments int
	CanCompact     bool
}

// Stats returns compaction statistics for a partition.
func (lc *LogCompactor) Stats(p *Partition) CompactionStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	frozen := 0
	for _, seg := range p.segments {
		if seg.frozen {
			frozen++
		}
	}
	return CompactionStats{
		FrozenSegments: frozen,
		CanCompact:     frozen >= 2 && lc.enabled,
	}
}

func (lc *LogCompactor) readAllRecords(seg *Segment) ([][]byte, error) {
	var records [][]byte
	if seg.file == nil {
		return nil, nil
	}

	info, err := seg.file.Stat()
	if err != nil {
		return nil, err
	}

	pos := int64(0)
	for pos < info.Size() {
		var lenBuf [4]byte
		if _, err := seg.file.ReadAt(lenBuf[:], pos); err != nil {
			break
		}
		dataLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
		if dataLen == 0 || dataLen > 16*1024*1024 {
			break // corrupt or invalid
		}
		data := make([]byte, dataLen)
		if _, err := seg.file.ReadAt(data, pos+4); err != nil {
			break
		}
		records = append(records, data)
		pos += 4 + dataLen
	}
	return records, nil
}
