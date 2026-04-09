package hot

import (
	"encoding/binary"
	"os"
	"sync"
)

// IndexEntry maps an offset to a byte position in a segment file.
type IndexEntry struct {
	Offset    uint64
	Position  uint32
	Timestamp int64
}

// SparseIndex maintains every Nth message's position for fast offset lookup.
type SparseIndex struct {
	mu       sync.RWMutex
	entries  []IndexEntry
	interval uint32
}

// Add inserts an index entry (caller ensures it's at the right interval).
func (si *SparseIndex) Add(offset uint64, position uint32, timestamp int64) {
	si.mu.Lock()
	si.entries = append(si.entries, IndexEntry{Offset: offset, Position: position, Timestamp: timestamp})
	si.mu.Unlock()
}

// Search finds the position for exact or nearest offset <= target.
func (si *SparseIndex) Search(targetOffset uint64) (uint32, bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()

	entries := si.entries
	lo, hi := 0, len(entries)-1

	for lo <= hi {
		mid := (lo + hi) / 2
		if entries[mid].Offset == targetOffset {
			return entries[mid].Position, true
		}
		if entries[mid].Offset < targetOffset {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return 0, false
}

// Entries returns a copy of index entries.
func (si *SparseIndex) Entries() []IndexEntry {
	return si.entries
}

// Len returns the number of index entries.
func (si *SparseIndex) Len() int {
	return len(si.entries)
}

// Save writes the index to a binary file.
func (si *SparseIndex) Save(path string) error {
	si.mu.RLock()
	defer si.mu.RUnlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 20) // 8 + 4 + 8 per entry
	for _, e := range si.entries {
		binary.BigEndian.PutUint64(buf[0:], e.Offset)
		binary.BigEndian.PutUint32(buf[8:], e.Position)
		binary.BigEndian.PutUint64(buf[12:], uint64(e.Timestamp))
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return f.Sync()
}

// Load reads the index from a binary file.
func (si *SparseIndex) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	entryCount := len(data) / 20
	si.mu.Lock()
	si.entries = make([]IndexEntry, 0, entryCount)
	for i := 0; i < entryCount; i++ {
		pos := i * 20
		si.entries = append(si.entries, IndexEntry{
			Offset:    binary.BigEndian.Uint64(data[pos:]),
			Position:  binary.BigEndian.Uint32(data[pos+8:]),
			Timestamp: int64(binary.BigEndian.Uint64(data[pos+12:])),
		})
	}
	si.mu.Unlock()
	return nil
}
