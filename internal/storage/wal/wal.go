package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	WALHeaderSize = 17 // type(1) + size(4) + timestamp(8) + crc(4)
)

// EntryType identifies the kind of WAL entry.
type EntryType uint8

const (
	EntryMessage    EntryType = 1
	EntryTopicMeta  EntryType = 2
	EntryCheckpoint EntryType = 3
)

// SyncMode controls when WAL writes are flushed to disk.
type SyncMode uint8

const (
	SyncImmediate SyncMode = iota
	SyncInterval
	SyncOS
)

// WAL provides durable write-ahead logging.
type WAL struct {
	mu           sync.Mutex
	dir          string
	activeFile   *os.File
	activeSize   int64
	maxSize      int64
	syncMode     SyncMode
	syncInterval time.Duration

	offset     uint64 // Global WAL byte offset
	segmentSeq uint64 // Current segment sequence number

	writeBuf *bufio.Writer

	syncTicker *time.Ticker
	syncStop   chan struct{}
	syncDone   chan struct{}
}

// NewWAL creates or opens a WAL in the given directory.
func NewWAL(dir string, maxSize int64, syncMode SyncMode, syncInterval time.Duration) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:          dir,
		maxSize:      maxSize,
		syncMode:     syncMode,
		syncInterval: syncInterval,
		syncStop:     make(chan struct{}),
		syncDone:     make(chan struct{}),
	}

	if err := w.openOrCreateSegment(); err != nil {
		return nil, err
	}

	if syncMode == SyncInterval {
		w.syncTicker = time.NewTicker(syncInterval)
		go w.syncLoop()
	}

	return w, nil
}

// Append writes an entry to the WAL. Returns the WAL byte offset.
func (w *WAL) Append(entryType EntryType, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entrySize := int64(WALHeaderSize + len(data))
	if w.activeSize+entrySize > w.maxSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	var header [WALHeaderSize]byte
	header[0] = byte(entryType)
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	binary.BigEndian.PutUint64(header[5:], uint64(time.Now().UnixNano()))

	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc.Write(header[:13])
	crc.Write(data)
	binary.BigEndian.PutUint32(header[13:], crc.Sum32())

	if _, err := w.writeBuf.Write(header[:]); err != nil {
		return 0, err
	}
	if _, err := w.writeBuf.Write(data); err != nil {
		return 0, err
	}

	offset := w.offset
	w.offset += uint64(entrySize)
	w.activeSize += entrySize

	if w.syncMode == SyncImmediate {
		if err := w.writeBuf.Flush(); err != nil {
			return 0, err
		}
		if err := w.activeFile.Sync(); err != nil {
			return 0, err
		}
	}

	return offset, nil
}

// Recover iterates all WAL entries for crash recovery.
func (w *WAL) Recover(fromOffset uint64, fn func(EntryType, []byte) error) error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, seg := range segments {
		f, err := os.Open(seg)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(f)

		for {
			var header [WALHeaderSize]byte
			if _, err := io.ReadFull(reader, header[:]); err != nil {
				break // EOF or partial write
			}

			dataSize := binary.BigEndian.Uint32(header[1:])
			storedCRC := binary.BigEndian.Uint32(header[13:])

			// Validate dataSize before allocation
			if dataSize > uint32(w.maxSize) || dataSize > 16*1024*1024 {
				break // Corrupt entry — stop recovery
			}

			data := make([]byte, dataSize)
			if _, err := io.ReadFull(reader, data); err != nil {
				break // Partial entry (crash recovery)
			}

			crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
			crc.Write(header[:13])
			crc.Write(data)
			if crc.Sum32() != storedCRC {
				break // Corruption — stop here
			}

			entryType := EntryType(header[0])
			if err := fn(entryType, data); err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
	}
	return nil
}

// Checkpoint marks the WAL position as durable.
func (w *WAL) Checkpoint(offset uint64) error {
	cpPath := filepath.Join(w.dir, "checkpoint")
	data := []byte(fmt.Sprintf("%d\n", offset))
	return os.WriteFile(cpPath, data, 0640)
}

// Truncate removes WAL segments fully before the checkpoint.
func (w *WAL) Truncate() error {
	cpOffset, err := w.readCheckpoint()
	if err != nil {
		return nil // No checkpoint yet
	}

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	if len(segments) <= 1 {
		return nil
	}

	for _, seg := range segments[:len(segments)-1] {
		segEnd := w.segmentEndOffset(seg)
		if segEnd <= cpOffset {
			os.Remove(seg)
		}
	}
	return nil
}

// Offset returns the current WAL byte offset.
func (w *WAL) Offset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Close flushes and closes the WAL.
func (w *WAL) Close() error {
	if w.syncTicker != nil {
		w.syncTicker.Stop()
		select {
		case <-w.syncStop:
			// already closed
		default:
			close(w.syncStop)
		}
		<-w.syncDone // Wait for syncLoop goroutine to finish
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writeBuf != nil {
		w.writeBuf.Flush()
		w.writeBuf = nil
	}
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		err := w.activeFile.Close()
		w.activeFile = nil
		return err
	}
	return nil
}

func (w *WAL) rotate() error {
	if w.writeBuf != nil {
		w.writeBuf.Flush()
	}
	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		w.activeFile.Close()
		w.activeFile = nil
	}

	w.segmentSeq++
	path := w.segmentPath(w.segmentSeq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return err
	}
	w.activeFile = f
	w.activeSize = 0
	w.writeBuf = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (w *WAL) openOrCreateSegment() error {
	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		last := segments[len(segments)-1]
		base := filepath.Base(last)
		seq, err := strconv.ParseUint(strings.TrimSuffix(base, ".wal"), 10, 64)
		if err != nil {
			return fmt.Errorf("parse WAL segment filename %q: %w", base, err)
		}
		w.segmentSeq = seq

		f, err := os.OpenFile(last, os.O_RDWR, 0640)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		w.activeFile = f
		w.activeSize = info.Size()
		w.offset += uint64(info.Size())
	} else {
		w.segmentSeq = 1
		path := w.segmentPath(w.segmentSeq)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
		if err != nil {
			return err
		}
		w.activeFile = f
		w.activeSize = 0
	}

	w.writeBuf = bufio.NewWriterSize(w.activeFile, 64*1024)
	return nil
}

func (w *WAL) segmentPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%012d.wal", seq))
}

func (w *WAL) listSegments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var segments []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			segments = append(segments, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Strings(segments)
	return segments, nil
}

func (w *WAL) readCheckpoint() (uint64, error) {
	data, err := os.ReadFile(filepath.Join(w.dir, "checkpoint"))
	if err != nil {
		return 0, err
	}
	var offset uint64
	n, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &offset)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid checkpoint file content")
	}
	return offset, nil
}

func (w *WAL) segmentEndOffset(path string) uint64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return uint64(info.Size()) // Approximate — used for truncation only
}

func (w *WAL) syncLoop() {
	defer close(w.syncDone)
	for {
		select {
		case <-w.syncTicker.C:
			w.mu.Lock()
			if w.writeBuf != nil {
				w.writeBuf.Flush()
			}
			if w.activeFile != nil {
				_ = w.activeFile.Sync()
			}
			w.mu.Unlock()
		case <-w.syncStop:
			return
		}
	}
}
