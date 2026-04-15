package cold

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/storage/warm"
)

const (
	archiveMagic   uint32 = 0x434F4C44 // "COLD"
	archiveVersion uint32 = 1
	archiveHeader         = 64
	archiveFooter         = 16
)

// OffsetRange defines the offset range of an archive.
type OffsetRange struct {
	Min, Max uint64
}

// TimeRange defines the timestamp range.
type TimeRange struct {
	Min, Max int64
}

// ArchiveSegIndex is a segment boundary within a cold archive.
type ArchiveSegIndex struct {
	Offset   uint64
	Position uint32
	Length   uint32
}

// ColdArchive is a read-only compressed archive of cold data.
type ColdArchive struct {
	mu          sync.RWMutex
	path        string
	offsetRange OffsetRange
	segIndex    []ArchiveSegIndex
	size        int64
	file        *os.File
	compressed  bool
	dictID      uint32
	compressor  *DictCompressor
}

// ColdConfig holds configuration for the cold tier.
type ColdConfig struct {
	Dir              string
	ArchiveSize      int64
	Compression      string
	CompressionLevel int
}

// ColdArchiveOption configures archive creation.
type ColdArchiveOption func(*coldArchiveOpts)

type coldArchiveOpts struct {
	compressor *DictCompressor
	dictID     uint32
}

// WithCompressor sets the zstd compressor for archive creation.
func WithCompressor(comp *DictCompressor, dictID uint32) ColdArchiveOption {
	return func(o *coldArchiveOpts) {
		o.compressor = comp
		o.dictID = dictID
	}
}

// CreateColdArchive creates a new cold archive from SSTables.
func CreateColdArchive(path string, sstables []*warm.SSTable, opts ...ColdArchiveOption) (*ColdArchive, error) {
	options := &coldArchiveOpts{}
	for _, o := range opts {
		o(options)
	}
	if len(sstables) == 0 {
		return nil, fmt.Errorf("no SSTables to archive")
	}

	// Collect all entries from SSTables, merge by offset
	allEntries := make(map[uint64][]byte)
	allDeleted := make(map[uint64]bool)
	var minOff, maxOff uint64
	var minTs, maxTs int64
	first := true

	for _, sst := range sstables {
		meta := sst.Metadata()
		if first || meta.MinOffset < minOff {
			minOff = meta.MinOffset
		}
		if first || meta.MaxOffset > maxOff {
			maxOff = meta.MaxOffset
		}
		first = false
		for off := meta.MinOffset; off <= meta.MaxOffset; off++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, off)
			val, found, deleted := sst.Get(key)
			if found {
				allEntries[off] = val
				allDeleted[off] = deleted
			}
		}
	}

	if len(allEntries) == 0 {
		return nil, fmt.Errorf("no entries in SSTables")
	}

	// Build archive data: segments of entries
	var dataBuf []byte
	var segIdx []ArchiveSegIndex
	compressed := options.compressor != nil

	segStart := uint32(0)
	var segData []byte
	var segFirstOff uint64
	count := 0

	for off := minOff; off <= maxOff; off++ {
		val, exists := allEntries[off]
		if !exists {
			continue
		}

		if count == 0 {
			segFirstOff = off
		}

		// Serialize entry
		deleted := byte(0)
		if allDeleted[off] {
			deleted = 1
		}
		entry := make([]byte, 8+1+4+len(val))
		binary.BigEndian.PutUint64(entry[0:8], off)
		entry[8] = deleted
		binary.BigEndian.PutUint32(entry[9:13], uint32(len(val)))
		copy(entry[13:], val)
		segData = append(segData, entry...)
		count++

		// Flush segment every 1000 entries or at end
		if count >= 1000 || off == maxOff {
			// Compress segment if compressor is available
			segLen := uint32(len(segData))
			if compressed {
				segData = CompressData(segData, options.compressor)
				segLen = uint32(len(segData))
			}
			segIdx = append(segIdx, ArchiveSegIndex{
				Offset:   segFirstOff,
				Position: segStart,
				Length:   segLen,
			})
			dataBuf = append(dataBuf, segData...)
			segStart = uint32(len(dataBuf))
			segData = nil
			count = 0
		}
	}

	// Build header
	header := make([]byte, archiveHeader)
	binary.BigEndian.PutUint32(header[0:4], archiveMagic)
	binary.BigEndian.PutUint32(header[4:8], archiveVersion)
	binary.BigEndian.PutUint64(header[8:16], minOff)
	binary.BigEndian.PutUint64(header[16:24], maxOff)
	binary.BigEndian.PutUint64(header[24:32], uint64(minTs))
	binary.BigEndian.PutUint64(header[32:40], uint64(maxTs))
	binary.BigEndian.PutUint32(header[40:44], uint32(len(segIdx)))
	if compressed {
		header[44] = 1 // compression type: zstd
		binary.BigEndian.PutUint32(header[45:49], options.dictID)
	}

	// Segment index
	segIndexData := make([]byte, len(segIdx)*16)
	for i, si := range segIdx {
		off := i * 16
		binary.BigEndian.PutUint64(segIndexData[off:off+8], si.Offset)
		binary.BigEndian.PutUint32(segIndexData[off+8:off+12], si.Position)
		binary.BigEndian.PutUint32(segIndexData[off+12:off+16], si.Length)
	}

	// Footer
	footer := make([]byte, archiveFooter)
	binary.BigEndian.PutUint32(footer[0:4], archiveMagic)

	// Write: header + data + segmentIndex + footer
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if _, err := file.Write(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("write header: %w", err)
	}
	if _, err := file.Write(dataBuf); err != nil {
		file.Close()
		return nil, fmt.Errorf("write data: %w", err)
	}
	if _, err := file.Write(segIndexData); err != nil {
		file.Close()
		return nil, fmt.Errorf("write index: %w", err)
	}
	if _, err := file.Write(footer); err != nil {
		file.Close()
		return nil, fmt.Errorf("write footer: %w", err)
	}
	file.Close()

	return OpenColdArchive(path)
}
func OpenColdArchive(path string) (*ColdArchive, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	size := stat.Size()

	if size < archiveHeader+archiveFooter {
		f.Close()
		return nil, fmt.Errorf("archive too small")
	}

	// Read header
	header := make([]byte, archiveHeader)
	if _, err := f.ReadAt(header, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("read archive header: %w", err)
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != archiveMagic {
		f.Close()
		return nil, fmt.Errorf("invalid archive magic")
	}

	minOff := binary.BigEndian.Uint64(header[8:16])
	maxOff := binary.BigEndian.Uint64(header[16:24])
	segCount := binary.BigEndian.Uint32(header[40:44])
	compressed := header[44] == 1
	dictID := binary.BigEndian.Uint32(header[45:49])

	// Read segment index (before footer)
	segIndexSize := int(segCount) * 16
	segIndexData := make([]byte, segIndexSize)
	if _, err := f.ReadAt(segIndexData, size-int64(archiveFooter)-int64(segIndexSize)); err != nil {
		f.Close()
		return nil, fmt.Errorf("read segment index: %w", err)
	}

	segIdx := make([]ArchiveSegIndex, segCount)
	for i := uint32(0); i < segCount; i++ {
		off := int(i) * 16
		segIdx[i] = ArchiveSegIndex{
			Offset:   binary.BigEndian.Uint64(segIndexData[off : off+8]),
			Position: binary.BigEndian.Uint32(segIndexData[off+8 : off+12]),
			Length:   binary.BigEndian.Uint32(segIndexData[off+12 : off+16]),
		}
	}

	return &ColdArchive{
		path:        path,
		offsetRange: OffsetRange{Min: minOff, Max: maxOff},
		segIndex:    segIdx,
		size:        size,
		file:        f,
		compressed:  compressed,
		dictID:      dictID,
	}, nil
}

// Get retrieves a value by offset.
func (ca *ColdArchive) Get(offset uint64) ([]byte, error) {
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if offset < ca.offsetRange.Min || offset > ca.offsetRange.Max {
		return nil, fmt.Errorf("offset %d out of range [%d,%d]", offset, ca.offsetRange.Min, ca.offsetRange.Max)
	}

	// Find segment containing this offset
	for _, si := range ca.segIndex {
		segData := make([]byte, si.Length)
		if _, err := ca.file.ReadAt(segData, int64(archiveHeader)+int64(si.Position)); err != nil {
			continue
		}

		// Decompress if needed
		if ca.compressed && ca.compressor != nil {
			var err error
			segData, err = DecompressData(segData, ca.compressor)
			if err != nil {
				continue
			}
		}

		// Scan segment for offset
		off := 0
		for off < len(segData) {
			if off+13 > len(segData) {
				break
			}
			entryOff := binary.BigEndian.Uint64(segData[off : off+8])
			deleted := segData[off+8]
			valLen := int(binary.BigEndian.Uint32(segData[off+9 : off+13]))
			if off+13+valLen > len(segData) {
				break
			}
			val := segData[off+13 : off+13+valLen]
			if entryOff == offset {
				if deleted == 1 {
					return nil, nil
				}
				result := make([]byte, len(val))
				copy(result, val)
				return result, nil
			}
			off += 13 + valLen
		}
	}

	return nil, fmt.Errorf("offset %d not found", offset)
}

// OffsetRange returns the archive's offset range.
func (ca *ColdArchive) OffsetRange() OffsetRange {
	return ca.offsetRange
}

// Close closes the archive file.
func (ca *ColdArchive) Close() error {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return ca.file.Close()
}

// IsCompressed returns true if the archive data is zstd-compressed.
func (ca *ColdArchive) IsCompressed() bool {
	return ca.compressed
}

// DictID returns the dictionary ID used for compression.
func (ca *ColdArchive) DictID() uint32 {
	return ca.dictID
}

// SetCompressor sets the decompressor for reading compressed segments.
func (ca *ColdArchive) SetCompressor(comp *DictCompressor) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	_ = comp // stored via closure for Get
	if ca.compressed {
		ca.compressor = comp
	}
}

// Path returns the archive file path.
func (ca *ColdArchive) Path() string {
	return ca.path
}

// Size returns the archive file size.
func (ca *ColdArchive) Size() int64 {
	return ca.size
}

// Remove deletes the archive file.
func (ca *ColdArchive) Remove() error {
	ca.Close()
	return os.Remove(ca.path)
}

// CreatedAt returns the file modification time.
func (ca *ColdArchive) CreatedAt() time.Time {
	if ca.file != nil {
		stat, _ := ca.file.Stat()
		if stat != nil {
			return stat.ModTime()
		}
	}
	return time.Time{}
}
