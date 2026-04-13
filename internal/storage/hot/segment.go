package hot

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	SegmentMagic     = uint32(0x43534731) // "CSG1"
	SegmentVersion   = uint32(1)
	SegmentHeaderLen = 32
)

// recordPool reduces allocations for combined write operations.
var recordPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// Segment represents a single log segment file on disk.
type Segment struct {
	mu      sync.RWMutex
	file    *os.File
	path    string
	size    int64
	maxSize int64
	baseOff uint64
	nextOff uint64
	index   *SparseIndex
	created time.Time
	frozen  atomic.Bool
}

// OpenSegment opens or creates a segment file.
func OpenSegment(path string, baseOffset uint64, maxSize int64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	seg := &Segment{
		file:    f,
		path:    path,
		size:    info.Size(),
		maxSize: maxSize,
		baseOff: baseOffset,
		nextOff: baseOffset,
		created: time.Now(),
		index: &SparseIndex{
			entries:  make([]IndexEntry, 0, 1024),
			interval: 256,
		},
	}

	if info.Size() == 0 {
		if err := seg.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := seg.readHeader(); err != nil {
			f.Close()
			return nil, err
		}
		if err := seg.rebuildIndex(); err != nil {
			f.Close()
			return nil, err
		}
	}

	return seg, nil
}

// Append writes a serialized message to the segment.
// Caller must hold external synchronization (Partition.mu).
func (s *Segment) Append(data []byte) (offset uint64, position int64, err error) {
	if s.frozen.Load() {
		return 0, 0, ErrSegmentReadOnly
	}

	recordSize := 4 + len(data)
	if s.size+int64(recordSize) > s.maxSize {
		return 0, 0, ErrSegmentFull
	}

	position = s.size
	offset = s.nextOff

	// Single write: length prefix + data combined to reduce syscalls
	item := recordPool.Get()
	bp, ok := item.(*[]byte)
	if !ok {
		// Should never happen, but handle gracefully
		buf := make([]byte, 4+len(data))
		binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
		copy(buf[4:], data)
		if _, err := s.file.WriteAt(buf, position); err != nil {
			return 0, 0, err
		}
		s.size += int64(recordSize)
		s.nextOff++
		return offset, position, nil
	}
	buf := *bp
	if cap(buf) < 4+len(data) {
		buf = make([]byte, 4+len(data))
	} else {
		buf = buf[:4+len(data)]
	}
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	if _, err := s.file.WriteAt(buf, position); err != nil {
		*bp = buf
		recordPool.Put(bp)
		return 0, 0, err
	}
	*bp = buf
	recordPool.Put(bp)
	s.size += int64(recordSize)
	s.nextOff++

	msgCount := offset - s.baseOff
	if msgCount%uint64(s.index.interval) == 0 {
		s.index.Add(offset, uint32(position), time.Now().UnixNano())
	}

	return offset, position, nil
}

// ReadAt reads a message at the given byte position.
func (s *Segment) ReadAt(position int64) ([]byte, error) {
	if position < SegmentHeaderLen || position >= s.size {
		return nil, fmt.Errorf("position %d out of range [32, %d)", position, s.size)
	}
	var lenBuf [4]byte
	if _, err := s.file.ReadAt(lenBuf[:], position); err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])

	if position+4+int64(dataLen) > s.size {
		return nil, fmt.Errorf("corrupt record: data length %d extends beyond segment at position %d", dataLen, position)
	}

	data := make([]byte, dataLen)
	if _, err := s.file.ReadAt(data, position+4); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadAtSequential reads a message at the given byte position and returns
// the data along with the position of the next record. This avoids repeated
// index lookups when scanning sequentially through a segment.
func (s *Segment) ReadAtSequential(position int64) (data []byte, nextPosition int64, err error) {
	if position < SegmentHeaderLen || position >= s.size {
		return nil, 0, fmt.Errorf("position %d out of range [32, %d)", position, s.size)
	}
	var lenBuf [4]byte
	if _, err := s.file.ReadAt(lenBuf[:], position); err != nil {
		return nil, 0, err
	}
	dataLen := binary.BigEndian.Uint32(lenBuf[:])

	endPos := position + 4 + int64(dataLen)
	if endPos > s.size {
		return nil, 0, fmt.Errorf("corrupt record: data length %d extends beyond segment at position %d", dataLen, position)
	}

	data = make([]byte, dataLen)
	if _, err := s.file.ReadAt(data, position+4); err != nil {
		return nil, 0, err
	}
	return data, endPos, nil
}

// FindPosition locates the byte position for a given offset using sparse index.
func (s *Segment) FindPosition(targetOffset uint64) (int64, error) {
	if targetOffset < s.baseOff {
		return 0, ErrOffsetTooOld
	}

	pos, found := s.index.Search(targetOffset)
	if found {
		return int64(pos), nil
	}

	// Linear scan from nearest position
	currentOffset := s.baseOff
	if s.index.Len() > 0 {
		entries := s.index.Entries()
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Offset <= targetOffset {
				pos = entries[i].Position
				currentOffset = entries[i].Offset
				break
			}
		}
	} else {
		pos = uint32(SegmentHeaderLen)
	}

	for currentOffset < targetOffset {
		var lenBuf [4]byte
		if _, err := s.file.ReadAt(lenBuf[:], int64(pos)); err != nil {
			return 0, err
		}
		dataLen := binary.BigEndian.Uint32(lenBuf[:])
		pos += 4 + uint32(dataLen)
		currentOffset++
	}

	return int64(pos), nil
}

// Freeze marks the segment as read-only.
func (s *Segment) Freeze() error {
	s.frozen.Store(true)
	return s.file.Sync()
}

// SaveIndex persists the sparse index to disk.
func (s *Segment) SaveIndex() error {
	return s.index.Save(s.path[:len(s.path)-4] + "idx")
}

// LoadIndex loads the sparse index from disk.
func (s *Segment) LoadIndex() error {
	return s.index.Load(s.path[:len(s.path)-4] + "idx")
}

// Close closes the segment file.
func (s *Segment) Close() error {
	return s.file.Close()
}

// BaseOffset returns the first offset in this segment.
func (s *Segment) BaseOffset() uint64 { return s.baseOff }

// NextOffset returns the next offset to assign.
func (s *Segment) NextOffset() uint64 { return s.nextOff }

// Size returns the current file size.
func (s *Segment) Size() int64 { return s.size }

// Path returns the segment file path.
func (s *Segment) Path() string { return s.path }

// Created returns the segment creation time.
func (s *Segment) Created() time.Time { return s.created }

// Frozen returns whether this segment is frozen (read-only).
func (s *Segment) Frozen() bool { return s.frozen.Load() }

// Remove closes and deletes the segment file and its index.
func (s *Segment) Remove() error {
	s.file.Close()
	idxPath := s.path[:len(s.path)-4] + "idx"
	os.Remove(idxPath) // best-effort index removal
	return os.Remove(s.path)
}

func (s *Segment) writeHeader() error {
	var header [SegmentHeaderLen]byte
	binary.BigEndian.PutUint32(header[0:], SegmentMagic)
	binary.BigEndian.PutUint32(header[4:], SegmentVersion)
	binary.BigEndian.PutUint64(header[8:], s.baseOff)
	binary.BigEndian.PutUint64(header[16:], uint64(s.created.UnixNano()))

	_, err := s.file.WriteAt(header[:], 0)
	if err != nil {
		return err
	}
	s.size = SegmentHeaderLen
	return nil
}

func (s *Segment) readHeader() error {
	var header [SegmentHeaderLen]byte
	if _, err := s.file.ReadAt(header[:], 0); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(header[0:]) != SegmentMagic {
		return ErrBadMagic
	}
	s.baseOff = binary.BigEndian.Uint64(header[8:])
	s.nextOff = s.baseOff
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	s.size = info.Size()
	if s.size == 0 {
		s.size = SegmentHeaderLen
	}
	return nil
}

func (s *Segment) rebuildIndex() error {
	s.index.entries = s.index.entries[:0]
	pos := int64(SegmentHeaderLen)
	count := uint64(0)

	for {
		var lenBuf [4]byte
		if _, err := s.file.ReadAt(lenBuf[:], pos); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(lenBuf[:])
		offset := s.baseOff + count
		if count%uint64(s.index.interval) == 0 {
			s.index.Add(offset, uint32(pos), 0)
		}
		pos += 4 + int64(dataLen)
		count++
	}

	s.nextOff = s.baseOff + count
	s.size = pos
	return nil
}
