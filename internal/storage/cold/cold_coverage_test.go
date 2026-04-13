package cold

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/warm"
)

func TestCreateColdArchiveEmptySSTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.dat")
	_, err := CreateColdArchive(path, nil)
	if err == nil {
		t.Error("expected error for empty SSTables")
	}
}

func TestOpenColdArchiveReadHeaderError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "truncated.dat")
	// Write exactly archiveHeader bytes (no footer) — size check passes? No, need >= header+footer
	// Actually size < header+footer triggers "archive too small"
	// To trigger read header error, we need a file that's >= header+footer but read fails.
	// On most OSes this is hard without special files.
	// Instead, create a valid-size file with segCount causing negative ReadAt offset.
	data := make([]byte, archiveHeader+archiveFooter)
	binary.BigEndian.PutUint32(data[0:4], archiveMagic)
	binary.BigEndian.PutUint32(data[4:8], archiveVersion)
	binary.BigEndian.PutUint32(data[40:44], 100) // segCount=100 -> negative ReadAt offset
	binary.BigEndian.PutUint32(data[archiveHeader:archiveHeader+4], archiveMagic)
	os.WriteFile(path, data, 0644)

	_, err := OpenColdArchive(path)
	if err == nil {
		t.Error("expected error when segment index read fails")
	}
}

func TestColdArchiveGetReadAtError(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{0, 5},
	})
	defer closeSSTables(ssts)

	archivePath := filepath.Join(dir, "archive.dat")
	ca, err := CreateColdArchive(archivePath, ssts)
	if err != nil {
		t.Fatal(err)
	}

	// Close the underlying file so ReadAt fails
	ca.file.Close()

	_, err = ca.Get(0)
	if err == nil {
		t.Error("expected error when file is closed")
	}
}

func TestColdArchiveGetNotFoundInSegment(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	os.MkdirAll(sstDir, 0755)

	// Create SSTable with offset 10
	mt := warm.NewMemTable(4096)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 10)
	mt.Put(key, []byte("val-10"))
	mt.Freeze()

	sst, err := warm.FlushMemTable(mt, sstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	archivePath := filepath.Join(dir, "archive.dat")
	ca, err := CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Close()

	// Offset 11 is within range [10,10]? No, 11 > 10 so it's out of range.
	// We need an offset that's within the range but not in the segment.
	// Since the archive only has offset 10, any other offset in [10,10] doesn't exist.
	// But wait, minOff=maxOff=10, so only 10 is in range.
	// To test "not found in segment", we'd need a gap within the range.
	// Create two SSTables: one with offset 10, one with offset 12. Offset 11 is in range but missing.
}

func TestColdArchiveGetMissingOffsetInRange(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	os.MkdirAll(sstDir, 0755)

	mt1 := warm.NewMemTable(4096)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 10)
	mt1.Put(key, []byte("val-10"))
	mt1.Freeze()
	sst1, _ := warm.FlushMemTable(mt1, sstDir)
	defer sst1.Close()

	mt2 := warm.NewMemTable(4096)
	key2 := make([]byte, 8)
	binary.BigEndian.PutUint64(key2, 12)
	mt2.Put(key2, []byte("val-12"))
	mt2.Freeze()
	sst2, _ := warm.FlushMemTable(mt2, sstDir)
	defer sst2.Close()

	archivePath := filepath.Join(dir, "archive.dat")
	ca, err := CreateColdArchive(archivePath, []*warm.SSTable{sst1, sst2})
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Close()

	_, err = ca.Get(11)
	if err == nil {
		t.Error("expected error for missing offset within range")
	}
}
