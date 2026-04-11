package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var errBinaryNotFound = errors.New("binary log file not found")

const (
	logMagic       = uint32(0x52414C54) // "RALT" (Raft Log)
	logVersion     = uint16(1)
	logHeaderSize  = 4 + 2 + 8 + 4 // magic + version + firstIndex + count = 18
	entryFixedSize = 8 + 8 + 4 + 4 // index + term + type + dataLen = 24
)

// encodeLogBinary serializes the raft log to binary format.
// Format: [magic][version][firstIndex][count][entries...]
// Per entry: [index][term][type][dataLen][data]
func encodeLogBinary(firstIndex Index, entries []LogEntry) []byte {
	size := logHeaderSize + len(entries)*entryFixedSize
	for _, e := range entries {
		size += len(e.Data)
	}

	buf := make([]byte, size)
	off := 0

	// Header
	binary.BigEndian.PutUint32(buf[off:], logMagic)
	off += 4
	binary.BigEndian.PutUint16(buf[off:], logVersion)
	off += 2
	binary.BigEndian.PutUint64(buf[off:], uint64(firstIndex))
	off += 8
	binary.BigEndian.PutUint32(buf[off:], uint32(len(entries)))
	off += 4

	// Entries
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[off:], uint64(e.Index))
		off += 8
		binary.BigEndian.PutUint64(buf[off:], uint64(e.Term))
		off += 8
		binary.BigEndian.PutUint32(buf[off:], uint32(e.Type))
		off += 4
		binary.BigEndian.PutUint32(buf[off:], uint32(len(e.Data)))
		off += 4
		copy(buf[off:], e.Data)
		off += len(e.Data)
	}

	return buf
}

// decodeLogBinary deserializes the raft log from binary format.
func decodeLogBinary(data []byte) (firstIndex Index, entries []LogEntry, err error) {
	if len(data) < logHeaderSize {
		return 0, nil, fmt.Errorf("raft log: data too short (%d bytes)", len(data))
	}

	off := 0

	// Validate magic
	magic := binary.BigEndian.Uint32(data[off:])
	off += 4
	if magic != logMagic {
		return 0, nil, fmt.Errorf("raft log: invalid magic 0x%08X", magic)
	}

	// Version
	ver := binary.BigEndian.Uint16(data[off:])
	off += 2
	if ver != logVersion {
		return 0, nil, fmt.Errorf("raft log: unsupported version %d", ver)
	}

	// FirstIndex
	firstIndex = Index(binary.BigEndian.Uint64(data[off:]))
	off += 8

	// Count
	count := binary.BigEndian.Uint32(data[off:])
	off += 4

	entries = make([]LogEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+entryFixedSize > len(data) {
			return 0, nil, fmt.Errorf("raft log: truncated entry %d", i)
		}

		var e LogEntry
		e.Index = Index(binary.BigEndian.Uint64(data[off:]))
		off += 8
		e.Term = Term(binary.BigEndian.Uint64(data[off:]))
		off += 8
		e.Type = EntryType(binary.BigEndian.Uint32(data[off:]))
		off += 4
		dataLen := binary.BigEndian.Uint32(data[off:])
		off += 4

		if off+int(dataLen) > len(data) {
			return 0, nil, fmt.Errorf("raft log: truncated data for entry %d (need %d, have %d)", i, dataLen, len(data)-off)
		}

		if dataLen > 0 {
			e.Data = make([]byte, dataLen)
			copy(e.Data, data[off:off+int(dataLen)])
			off += int(dataLen)
		}
		entries = append(entries, e)
	}

	return firstIndex, entries, nil
}

// saveLogBinary persists the raft log using binary format with atomic write.
func (l *RaftLog) saveLogBinary() error {
	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return err
	}

	l.mu.RLock()
	data := encodeLogBinary(l.firstIndex, l.entries)
	l.mu.RUnlock()

	path := filepath.Join(l.dir, "log.bin")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadLogBinary restores the raft log from binary format.
func (l *RaftLog) loadLogBinary() error {
	path := filepath.Join(l.dir, "log.bin")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return errBinaryNotFound
		}
		return err
	}
	if len(data) == 0 {
		return errBinaryNotFound
	}

	firstIndex, entries, err := decodeLogBinary(data)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.firstIndex = firstIndex
	l.entries = entries
	l.mu.Unlock()
	return nil
}
