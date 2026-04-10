package warm

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sstMagic      uint32 = 0x53535442 // "SSTB"
	sstVersion    uint32 = 1
	sstFooterSize        = 48
)

// CompressionType controls SSTable block compression.
type CompressionType uint8

const (
	CompressNone CompressionType = 0
	CompressZstd CompressionType = 1
)

// SSTMetadata holds metadata about an SSTable.
type SSTMetadata struct {
	MinOffset    uint64
	MaxOffset    uint64
	MinTimestamp int64
	MaxTimestamp int64
	EntryCount   uint32
	BloomOffset  uint32
	IndexOffset  uint32
	Compression  CompressionType
}

// SSTable is an immutable sorted on-disk file.
type SSTable struct {
	mu     sync.RWMutex
	file   *os.File
	path   string
	index  *BlockIndex
	bloom  *BloomFilter
	meta   SSTMetadata
	closed bool
}

// FlushMemTable writes a frozen MemTable to a new SSTable file.
func FlushMemTable(mt *MemTable, dir string) (*SSTable, error) {
	it := mt.Iterator()
	if !it.Next() {
		// Empty memtable — create a minimal SSTable
		return createEmptySSTable(dir)
	}

	// Collect all entries
	entries := make([]MemTableEntry, 0)
	entries = append(entries, it.Entry())
	for it.Next() {
		entries = append(entries, it.Entry())
	}

	// Build bloom filter from all keys
	bloom := NewBloomFilter(uint32(len(entries)), 0.01)
	for _, e := range entries {
		bloom.Add(e.Key)
	}

	// Write data blocks, track block boundaries
	var dataBuf []byte
	blockIndex := NewBlockIndex()
	blockStart := uint32(0)
	var blockData []byte

	firstKeyOfBlock := entries[0].Key
	var minOff, maxOff uint64
	var minTs, maxTs int64

	for i, e := range entries {
		// Track metadata
		if len(e.Key) == 8 {
			off := binary.BigEndian.Uint64(e.Key)
			if i == 0 || off < minOff {
				minOff = off
			}
			if i == 0 || off > maxOff {
				maxOff = off
			}
		}
		if e.Timestamp < minTs || i == 0 {
			minTs = e.Timestamp
		}
		if e.Timestamp > maxTs || i == 0 {
			maxTs = e.Timestamp
		}

		// Serialize entry
		entryData := serializeEntry(e)
		blockData = append(blockData, entryData...)

		// Flush block if it exceeds blockSizeDefault
		if len(blockData) >= blockSizeDefault || i == len(entries)-1 {
			blockIndex.Add(firstKeyOfBlock, blockStart, uint32(len(blockData)))
			dataBuf = append(dataBuf, blockData...)
			blockStart = uint32(len(dataBuf))
			blockData = nil
			if i+1 < len(entries) {
				firstKeyOfBlock = entries[i+1].Key
			}
		}
	}

	// Serialize bloom and index
	bloomData, _ := bloom.MarshalBinary()
	bloomOffset := uint32(len(dataBuf))
	dataBuf = append(dataBuf, bloomData...)

	indexData, _ := blockIndex.MarshalBinary()
	indexOffset := uint32(len(dataBuf))
	dataBuf = append(dataBuf, indexData...)

	// Build footer
	footer := make([]byte, sstFooterSize)
	binary.BigEndian.PutUint32(footer[0:4], sstMagic)
	binary.BigEndian.PutUint32(footer[4:8], sstVersion)
	binary.BigEndian.PutUint32(footer[8:12], bloomOffset)
	binary.BigEndian.PutUint32(footer[12:16], indexOffset)
	binary.BigEndian.PutUint32(footer[16:20], uint32(len(entries)))
	binary.BigEndian.PutUint64(footer[20:28], minOff)
	binary.BigEndian.PutUint64(footer[28:36], maxOff)
	footer[36] = byte(CompressNone)

	// Write atomically
	path := tempSSTPath(dir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, append(dataBuf, footer...), 0644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return nil, err
	}

	return OpenSSTable(path)
}

// OpenSSTable opens an existing SSTable.
func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if stat.Size() < int64(sstFooterSize) {
		f.Close()
		return nil, errBadSSTable
	}

	// Read footer
	footer := make([]byte, sstFooterSize)
	if _, err := f.ReadAt(footer, stat.Size()-int64(sstFooterSize)); err != nil {
		f.Close()
		return nil, err
	}

	magic := binary.BigEndian.Uint32(footer[0:4])
	if magic != sstMagic {
		f.Close()
		return nil, errBadSSTable
	}

	bloomOffset := binary.BigEndian.Uint32(footer[8:12])
	indexOffset := binary.BigEndian.Uint32(footer[12:16])
	entryCount := binary.BigEndian.Uint32(footer[16:20])
	minOff := binary.BigEndian.Uint64(footer[20:28])
	maxOff := binary.BigEndian.Uint64(footer[28:36])
	comp := CompressionType(footer[36])

	// Read bloom filter
	dataSize := stat.Size() - int64(sstFooterSize)
	data := make([]byte, dataSize)
	if _, err := f.ReadAt(data, 0); err != nil {
		f.Close()
		return nil, err
	}

	bloom := &BloomFilter{}
	if bloomOffset > 0 && bloomOffset < uint32(dataSize) {
		bloomEnd := indexOffset
		if bloomEnd > uint32(dataSize) {
			bloomEnd = uint32(dataSize)
		}
		bloom.UnmarshalBinary(data[bloomOffset:bloomEnd])
	}

	// Read block index
	index, _ := UnmarshalBlockIndex(data[indexOffset:])

	return &SSTable{
		file:  f,
		path:  path,
		index: index,
		bloom: bloom,
		meta: SSTMetadata{
			MinOffset:   minOff,
			MaxOffset:   maxOff,
			EntryCount:  entryCount,
			BloomOffset: bloomOffset,
			IndexOffset: indexOffset,
			Compression: comp,
		},
	}, nil
}

// Get looks up a key in the SSTable.
func (sst *SSTable) Get(key []byte) ([]byte, bool, bool) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return nil, false, false
	}

	// Check bloom filter first
	if sst.bloom != nil && !sst.bloom.MayContain(key) {
		return nil, false, false
	}

	// Find block
	block, ok := sst.index.Search(key)
	if !ok {
		return nil, false, false
	}

	// Read block data
	dataSize, _ := sst.file.Seek(0, 2)
	allData := make([]byte, dataSize)
	sst.file.ReadAt(allData, 0)

	blockEnd := block.Offset + block.Length
	if blockEnd > uint32(len(allData)) {
		blockEnd = uint32(len(allData))
	}
	blockData := allData[block.Offset:blockEnd]

	// Scan block for key
	return scanBlockForKey(blockData, key)
}

// Metadata returns the SSTable metadata.
func (sst *SSTable) Metadata() SSTMetadata {
	return sst.meta
}

// Path returns the file path.
func (sst *SSTable) Path() string {
	return sst.path
}

// Close closes the SSTable file.
func (sst *SSTable) Close() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()
	sst.closed = true
	return sst.file.Close()
}

// Remove deletes the SSTable file.
func (sst *SSTable) Remove() error {
	sst.Close()
	return os.Remove(sst.path)
}

func serializeEntry(e MemTableEntry) []byte {
	// [keyLen:4][key][deleted:1][valueLen:4][value][timestamp:8]
	var deleted byte
	if e.Deleted {
		deleted = 1
	}
	buf := make([]byte, 4+len(e.Key)+1+4+len(e.Value)+8)
	off := 0
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.Key)))
	off += 4
	copy(buf[off:], e.Key)
	off += len(e.Key)
	buf[off] = deleted
	off++
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(e.Value)))
	off += 4
	copy(buf[off:], e.Value)
	off += len(e.Value)
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(e.Timestamp))
	return buf
}

func scanBlockForKey(blockData []byte, key []byte) ([]byte, bool, bool) {
	off := 0
	for off < len(blockData) {
		if off+4 > len(blockData) {
			break
		}
		keyLen := int(binary.BigEndian.Uint32(blockData[off : off+4]))
		off += 4
		if off+keyLen > len(blockData) {
			break
		}
		entryKey := blockData[off : off+keyLen]
		off += keyLen
		if off+1 > len(blockData) {
			break
		}
		deleted := blockData[off] == 1
		off++
		if off+4 > len(blockData) {
			break
		}
		valLen := int(binary.BigEndian.Uint32(blockData[off : off+4]))
		off += 4
		if off+valLen > len(blockData) {
			break
		}
		value := blockData[off : off+valLen]
		off += valLen
		if off+8 > len(blockData) {
			// Last entry might not have full timestamp
			off = len(blockData)
		} else {
			off += 8 // skip timestamp
		}

		if compareKeys(entryKey, key) == 0 {
			return value, true, deleted
		}
	}
	return nil, false, false
}

func createEmptySSTable(dir string) (*SSTable, error) {
	footer := make([]byte, sstFooterSize)
	binary.BigEndian.PutUint32(footer[0:4], sstMagic)
	binary.BigEndian.PutUint32(footer[4:8], sstVersion)

	path := tempSSTPath(dir)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, footer, 0644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return nil, err
	}
	return OpenSSTable(path)
}

var sstSeq uint64

func tempSSTPath(dir string) string {
	seq := atomic.AddUint64(&sstSeq, 1)
	return fmt.Sprintf("%s/sst-%08d-%08x.dat", dir, seq, time.Now().UnixNano())
}
