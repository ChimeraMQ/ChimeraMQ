package cold

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chimeramq/chimera/internal/storage/warm"
	"github.com/klauspost/compress/zstd"
)

const (
	defaultDictThreshold = 100  // train dictionary every N archives
	defaultDictSamples   = 100  // max samples per training
	defaultDictMaxSize   = 1024 // max sample size in bytes
)

// ZstdDictTrainer collects message samples from SSTables and tracks archive
// counts for dictionary training. When the archive count reaches the configured
// threshold, it attempts to train a zstd dictionary from collected samples.
//
// Note: zstd.BuildDict in klauspost/compress has known issues with highly
// compressible data. When training fails, the archive still uses standard
// zstd compression (without a custom dictionary).
type ZstdDictTrainer struct {
	dir        string
	samples    [][]byte
	threshold  int
	maxSamples int
	maxSize    int
	dictID     uint32
}

// NewZstdDictTrainer creates a new dictionary trainer.
func NewZstdDictTrainer(dir string) *ZstdDictTrainer {
	return &ZstdDictTrainer{
		dir:        dir,
		threshold:  defaultDictThreshold,
		maxSamples: defaultDictSamples,
		maxSize:    defaultDictMaxSize,
		dictID:     1,
	}
}

// AddSamples extracts value samples from SSTables for dictionary training.
func (t *ZstdDictTrainer) AddSamples(sstables []*warm.SSTable) int {
	count := 0
	for _, sst := range sstables {
		if count >= t.maxSamples {
			break
		}
		meta := sst.Metadata()
		step := uint64(1)
		if meta.MaxOffset > meta.MinOffset {
			step = (meta.MaxOffset - meta.MinOffset) / uint64(t.maxSamples)
			if step == 0 {
				step = 1
			}
		}
		for off := meta.MinOffset; off <= meta.MaxOffset && count < t.maxSamples; off += step {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, off)
			val, found, deleted := sst.Get(key)
			if found && !deleted && len(val) > 0 {
				sample := val
				if len(sample) > t.maxSize {
					sample = sample[:t.maxSize]
				}
				cpy := make([]byte, len(sample))
				copy(cpy, sample)
				t.samples = append(t.samples, cpy)
				count++
			}
		}
	}
	return count
}

// ShouldTrain returns true if enough archives and samples exist to train.
func (t *ZstdDictTrainer) ShouldTrain(archiveCount int) bool {
	return archiveCount > 0 && archiveCount%t.threshold == 0 && len(t.samples) >= 10
}

// Train trains a zstd dictionary from collected samples and saves it to disk.
// Returns nil if training fails (graceful degradation to standard compression).
func (t *ZstdDictTrainer) Train() ([]byte, error) {
	samples := t.samples
	t.samples = nil

	if len(samples) < 10 {
		return nil, fmt.Errorf("not enough samples for dictionary training (need >= 10, got %d)", len(samples))
	}

	// Build history from all samples concatenated
	history := make([]byte, 0, len(samples)*t.maxSize)
	for _, s := range samples {
		history = append(history, s...)
	}

	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       uint32(t.dictID),
		Contents: samples,
		History:  history,
	})
	if err != nil {
		return nil, fmt.Errorf("build zstd dict: %w", err)
	}

	dictPath := t.DictPath()
	if err := os.WriteFile(dictPath, dict, 0644); err != nil {
		return nil, fmt.Errorf("write dictionary: %w", err)
	}

	return dict, nil
}

// LoadDict loads a trained dictionary from disk.
func (t *ZstdDictTrainer) LoadDict() ([]byte, error) {
	data, err := os.ReadFile(t.DictPath())
	if err != nil {
		return nil, err
	}
	return data, nil
}

// DictPath returns the dictionary file path.
func (t *ZstdDictTrainer) DictPath() string {
	return filepath.Join(t.dir, "zstd.dict")
}

// HasDict returns true if a trained dictionary exists on disk.
func (t *ZstdDictTrainer) HasDict() bool {
	_, err := os.Stat(t.DictPath())
	return err == nil
}

// DictCompressor provides zstd encode/decode with an optional dictionary.
type DictCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewDictCompressor creates a compressor, optionally using a trained dictionary.
func NewDictCompressor(dict []byte) (*DictCompressor, error) {
	var opts []zstd.EOption
	var dopts []zstd.DOption
	if len(dict) > 0 {
		opts = append(opts, zstd.WithEncoderDict(dict))
		dopts = append(dopts, zstd.WithDecoderDicts(dict))
	}
	opts = append(opts, zstd.WithEncoderLevel(zstd.SpeedDefault))

	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, dopts...)
	if err != nil {
		enc.Close()
		return nil, err
	}
	return &DictCompressor{encoder: enc, decoder: dec}, nil
}

// Compress compresses data.
func (c *DictCompressor) Compress(data []byte) []byte {
	return c.encoder.EncodeAll(data, nil)
}

// Decompress decompresses data.
func (c *DictCompressor) Decompress(data []byte) ([]byte, error) {
	return c.decoder.DecodeAll(data, nil)
}

// Close releases resources.
func (c *DictCompressor) Close() {
	if c.encoder != nil {
		c.encoder.Close()
	}
	if c.decoder != nil {
		c.decoder.Close()
	}
}

// CompressData compresses raw bytes with a 4-byte original-length prefix.
func CompressData(data []byte, comp *DictCompressor) []byte {
	compressed := comp.Compress(data)
	buf := make([]byte, 4+len(compressed))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], compressed)
	return buf
}

// DecompressData decompresses a 4-byte-prefixed compressed blob.
func DecompressData(data []byte, comp *DictCompressor) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("compressed data too small")
	}
	origLen := binary.BigEndian.Uint32(data[0:4])
	compressed := data[4:]
	decompressed, err := comp.Decompress(compressed)
	if err != nil {
		return nil, err
	}
	if uint32(len(decompressed)) != origLen {
		return nil, fmt.Errorf("decompressed size mismatch: got %d, want %d", len(decompressed), origLen)
	}
	return decompressed, nil
}

// CompressSegments compresses multiple segments into a single framed buffer.
func CompressSegments(segments [][]byte, comp *DictCompressor) []byte {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(segments)))
	for _, seg := range segments {
		compressed := comp.Compress(seg)
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(seg)))
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(compressed)))
		buf.Write(compressed)
	}
	return buf.Bytes()
}

// DecompressSegments decompresses a framed compressed buffer.
func DecompressSegments(data []byte, comp *DictCompressor) ([][]byte, error) {
	r := bytes.NewReader(data)
	var segCount uint32
	if err := binary.Read(r, binary.BigEndian, &segCount); err != nil {
		return nil, err
	}
	results := make([][]byte, 0, segCount)
	for i := uint32(0); i < segCount; i++ {
		var origLen, compLen uint32
		if err := binary.Read(r, binary.BigEndian, &origLen); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &compLen); err != nil {
			return nil, err
		}
		compressed := make([]byte, compLen)
		if _, err := r.Read(compressed); err != nil {
			return nil, err
		}
		decompressed, err := comp.Decompress(compressed)
		if err != nil {
			return nil, err
		}
		results = append(results, decompressed)
	}
	return results, nil
}
