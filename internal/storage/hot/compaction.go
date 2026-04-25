package hot

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// Decryptor is the interface needed to decrypt stored messages.
type Decryptor interface {
	Decrypt(ciphertext []byte, segmentID string) ([]byte, error)
}

// MaxCompactionKeys limits the number of unique keys during compaction
// to prevent unbounded memory growth. Default 1 million keys.
const MaxCompactionKeys = 1_000_000

// CompactionMode controls how log compaction works.
type CompactionMode int

const (
	CompactNone     CompactionMode = iota
	CompactKeyBased                // Keep latest value per key
)

// LogCompactor performs log compaction on a partition.
type LogCompactor struct {
	mu        sync.Mutex
	mode      CompactionMode
	enabled   bool
	encryptor Decryptor // optional, for at-rest decryption
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
		if seg.frozen.Load() {
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

	// Phase 1: snapshot frozen segments under lock
	p.mu.RLock()
	var frozen []*Segment
	for _, seg := range p.segments {
		if seg.frozen.Load() {
			frozen = append(frozen, seg)
		}
	}
	p.mu.RUnlock()

	if len(frozen) < 2 {
		return nil // nothing to compact
	}

	// Phase 2: read and compact (no lock needed — frozen segments are immutable)
	latest := make(map[string][]byte) // routingKey → raw data
	var keyless [][]byte              // messages without routing key
	totalRead := 0
	keysExceeded := false

	segID := p.Topic() + "/" + fmt.Sprintf("%d", p.PartitionID())

	for _, seg := range frozen {
		records, err := lc.readAllRecords(seg)
		if err != nil {
			continue
		}
		for _, data := range records {
			totalRead++
			if lc.encryptor != nil {
				decrypted, derr := lc.encryptor.Decrypt(data, segID)
				if derr != nil {
					continue
				}
				data = decrypted
			}
			env, err := message.Unmarshal(data)
			if err != nil {
				continue
			}
			if env.RoutingKey == "" {
				keyless = append(keyless, data)
			} else {
				// Check if we've exceeded the maximum keys limit
				if !keysExceeded {
					if len(latest) >= MaxCompactionKeys {
						keysExceeded = true
						slog.Warn("compaction key limit exceeded, skipping new unique keys",
							"limit", MaxCompactionKeys,
							"routing_key", env.RoutingKey)
					}
				}
				if !keysExceeded {
					latest[env.RoutingKey] = data
				}
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

	// Write segment header
	var header [SegmentHeaderLen]byte
	binary.BigEndian.PutUint32(header[0:], SegmentMagic)
	binary.BigEndian.PutUint32(header[4:], SegmentVersion)
	binary.BigEndian.PutUint64(header[8:], frozen[0].BaseOffset())
	binary.BigEndian.PutUint64(header[16:], uint64(time.Now().UnixNano()))
	if _, err := compactedFile.Write(header[:]); err != nil {
		compactedFile.Close()
		os.Remove(compactedPath)
		return fmt.Errorf("write compacted header: %w", err)
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

	for _, data := range keyless {
		if err := writeRecord(data); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return err
		}
	}
	for _, data := range latest {
		if err := writeRecord(data); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return err
		}
	}
	if err := compactedFile.Sync(); err != nil {
		slog.Error("compaction sync", "err", err)
	}
	compactedFile.Close()

	// Phase 3: swap segments under write lock
	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-verify frozen segments still exist (no concurrent changes)
	var newSegments []*Segment
	for _, seg := range p.segments {
		if seg.frozen.Load() {
			seg.Close()
			os.Remove(seg.file.Name())
			indexPath := seg.file.Name()[:len(seg.file.Name())-4] + ".idx"
			os.Remove(indexPath)
		} else {
			newSegments = append(newSegments, seg)
		}
	}

	compactedSeg, err := OpenSegment(compactedPath, frozen[0].BaseOffset(), p.maxSegSize)
	if err != nil {
		return fmt.Errorf("open compacted segment: %w", err)
	}
	compactedSeg.frozen.Store(true)

	rebuilt := []*Segment{compactedSeg}
	rebuilt = append(rebuilt, newSegments...)
	p.segments = rebuilt

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
		if seg.frozen.Load() {
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

	pos := int64(SegmentHeaderLen)
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
