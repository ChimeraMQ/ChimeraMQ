package hot

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Partition manages multiple segments for a single topic-partition.
type Partition struct {
	mu         sync.RWMutex
	topicName  string
	partitionID uint32
	dir        string
	segments   []*Segment
	active     *Segment
	highWater  uint64
	logStart   uint64
	maxSegSize int64
}

// OpenPartition loads or creates a partition's segment chain.
func OpenPartition(dir, topic string, id uint32, maxSegSize int64) (*Partition, error) {
	partDir := filepath.Join(dir, fmt.Sprintf("partition-%d", id))
	if err := os.MkdirAll(partDir, 0750); err != nil {
		return nil, err
	}

	p := &Partition{
		topicName:   topic,
		partitionID: id,
		dir:         partDir,
		segments:    make([]*Segment, 0),
		maxSegSize:  maxSegSize,
	}

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	if len(p.segments) == 0 {
		if err := p.createNewSegment(0); err != nil {
			return nil, err
		}
	}
	p.active = p.segments[len(p.segments)-1]
	if p.active.NextOffset() > 0 {
		p.highWater = p.active.NextOffset() - 1
	}

	return p, nil
}

// Append writes data to the active segment, rolling over if full.
func (p *Partition) Append(data []byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset, _, err := p.active.Append(data)
	if err == ErrSegmentFull {
		if freezeErr := p.active.Freeze(); freezeErr != nil {
			return 0, fmt.Errorf("freeze segment: %w", freezeErr)
		}
		if idxErr := p.active.SaveIndex(); idxErr != nil {
			return 0, fmt.Errorf("save index: %w", idxErr)
		}

		newBase := p.active.NextOffset()
		if err := p.createNewSegment(newBase); err != nil {
			return 0, err
		}
		p.active = p.segments[len(p.segments)-1]
		offset, _, err = p.active.Append(data)
		if err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	}

	p.highWater = offset
	return offset, nil
}

// Read reads a message at the given offset.
func (p *Partition) Read(offset uint64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seg := p.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found", offset)
	}

	pos, err := seg.FindPosition(offset)
	if err != nil {
		return nil, err
	}
	return seg.ReadAt(pos)
}

// ReadRange reads messages from start to end offset (inclusive).
func (p *Partition) ReadRange(startOffset, endOffset uint64, maxMessages int) ([][]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var results [][]byte
	for offset := startOffset; offset <= endOffset && len(results) < maxMessages; offset++ {
		seg := p.findSegment(offset)
		if seg == nil {
			break
		}
		pos, err := seg.FindPosition(offset)
		if err != nil {
			break
		}
		data, err := seg.ReadAt(pos)
		if err != nil {
			break
		}
		results = append(results, data)
	}
	return results, nil
}

// HighWatermark returns the highest committed offset.
func (p *Partition) HighWatermark() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.highWater
}

// LogStartOffset returns the earliest available offset.
func (p *Partition) LogStartOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.segments) == 0 {
		return 0
	}
	return p.segments[0].BaseOffset()
}

// Close closes all segments.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}

func (p *Partition) findSegment(offset uint64) *Segment {
	lo, hi := 0, len(p.segments)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		seg := p.segments[mid]
		if offset >= seg.BaseOffset() && offset < seg.NextOffset() {
			return seg
		}
		if offset < seg.BaseOffset() {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return nil
}

func (p *Partition) createNewSegment(baseOffset uint64) error {
	name := fmt.Sprintf("%020d.log", baseOffset)
	path := filepath.Join(p.dir, name)
	seg, err := OpenSegment(path, baseOffset, p.maxSegSize)
	if err != nil {
		return err
	}
	p.segments = append(p.segments, seg)
	return nil
}

func (p *Partition) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil // Empty dir is OK
	}

	for _, e := range entries {
		if e.IsDir() || len(e.Name()) < 5 || e.Name()[len(e.Name())-4:] != ".log" {
			continue
		}
		var baseOff uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &baseOff); err != nil {
			continue // skip malformed filenames
		}

		path := filepath.Join(p.dir, e.Name())
		seg, err := OpenSegment(path, baseOff, p.maxSegSize)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}
	return nil
}

// ReadAt reads a raw record at a byte position in the active segment (internal helper).
func (p *Partition) ReadAt(position int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active.ReadAt(position)
}

// SegmentCount returns the number of segments.
func (p *Partition) SegmentCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.segments)
}

// ReadRecord reads a length-prefixed record at a byte position.
func ReadRecordAt(f *os.File, position int64) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := f.ReadAt(lenBuf[:], position); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, dataLen)
	if _, err := f.ReadAt(data, position+4); err != nil {
		return nil, err
	}
	return data, nil
}
