package warm

import (
	"encoding/binary"
)

const (
	blockSizeDefault = 64 * 1024 // 64KB blocks
)

// BlockIndex maps first-key → block offset for SSTable lookups.
type BlockIndex struct {
	entries []BlockEntry
}

// BlockEntry points to a data block within an SSTable.
type BlockEntry struct {
	FirstKey []byte
	Offset   uint32
	Length   uint32
}

// NewBlockIndex creates an empty block index.
func NewBlockIndex() *BlockIndex {
	return &BlockIndex{}
}

// Add appends a block entry.
func (bi *BlockIndex) Add(firstKey []byte, offset, length uint32) {
	bi.entries = append(bi.entries, BlockEntry{
		FirstKey: firstKey,
		Offset:   offset,
		Length:   length,
	})
}

// Entries returns all block entries.
func (bi *BlockIndex) Entries() []BlockEntry {
	return bi.entries
}

// Search finds the block that might contain the given key using binary search.
// Returns the block entry, or the last block if key is beyond all entries.
func (bi *BlockIndex) Search(key []byte) (BlockEntry, bool) {
	if len(bi.entries) == 0 {
		return BlockEntry{}, false
	}

	lo, hi := 0, len(bi.entries)-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		cmp := compareKeys(bi.entries[mid].FirstKey, key)
		if cmp <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return bi.entries[lo], true
}

// Len returns the number of blocks.
func (bi *BlockIndex) Len() int {
	return len(bi.entries)
}

// MarshalBinary serializes the block index.
// Format: [count:4][entry1][entry2]...
// Each entry: [keyLen:4][key][offset:4][length:4]
func (bi *BlockIndex) MarshalBinary() ([]byte, error) {
	size := 4 // count
	for _, e := range bi.entries {
		size += 4 + len(e.FirstKey) + 4 + 4
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(bi.entries)))
	off := 4
	for _, e := range bi.entries {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.FirstKey)))
		off += 4
		copy(buf[off:], e.FirstKey)
		off += len(e.FirstKey)
		binary.BigEndian.PutUint32(buf[off:off+4], e.Offset)
		off += 4
		binary.BigEndian.PutUint32(buf[off:off+4], e.Length)
		off += 4
	}
	return buf, nil
}

// UnmarshalBlockIndex deserializes a block index.
func UnmarshalBlockIndex(data []byte) (*BlockIndex, error) {
	if len(data) < 4 {
		return nil, errBadBlockIndex
	}
	count := int(binary.BigEndian.Uint32(data[0:4]))
	bi := &BlockIndex{
		entries: make([]BlockEntry, 0, count),
	}
	off := 4
	for i := 0; i < count; i++ {
		if off+4 > len(data) {
			return nil, errBadBlockIndex
		}
		keyLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if off+keyLen+8 > len(data) {
			return nil, errBadBlockIndex
		}
		key := make([]byte, keyLen)
		copy(key, data[off:off+keyLen])
		off += keyLen
		blockOffset := binary.BigEndian.Uint32(data[off : off+4])
		off += 4
		blockLength := binary.BigEndian.Uint32(data[off : off+4])
		off += 4
		bi.entries = append(bi.entries, BlockEntry{
			FirstKey: key,
			Offset:   blockOffset,
			Length:   blockLength,
		})
	}
	return bi, nil
}

func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
