package cold

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/warm"
)

func TestColdArchiveAccessors(t *testing.T) {
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

	if ca.Path() != archivePath {
		t.Errorf("Path = %q, want %q", ca.Path(), archivePath)
	}

	if ca.Size() <= 0 {
		t.Error("Size should be positive")
	}

	if ca.CreatedAt().IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	ca.Close()

	if err := ca.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
		t.Error("archive file should be removed")
	}
}

func TestOpenColdArchiveTooSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "small.dat")
	os.WriteFile(path, []byte("tiny"), 0644)

	_, err := OpenColdArchive(path)
	if err == nil {
		t.Error("expected error for too-small archive")
	}
}

func TestOpenColdArchiveBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badmagic.dat")

	// Write a file that's big enough but has wrong magic
	data := make([]byte, archiveHeader+archiveFooter+100)
	os.WriteFile(path, data, 0644)

	_, err := OpenColdArchive(path)
	if err == nil {
		t.Error("expected error for bad magic")
	}
}

func TestOpenColdArchiveMissingFile(t *testing.T) {
	_, err := OpenColdArchive("/nonexistent/path/archive.dat")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestColdArchiveGetNotFound(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	os.MkdirAll(sstDir, 0755)

	// Create a memtable with only offset 10
	mt := warm.NewMemTable(4096)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 10)
	mt.Put(key, []byte("val-10"))
	mt.Freeze()

	sst, err := warm.FlushMemTable(mt, sstDir, 0)
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

	// Offset 11 doesn't exist in the archive
	_, err = ca.Get(11)
	if err == nil {
		t.Error("expected error for missing offset")
	}
}

func TestColdArchiveGetOutOfRange(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{100, 105},
	})
	defer closeSSTables(ssts)

	archivePath := filepath.Join(dir, "archive.dat")
	ca, err := CreateColdArchive(archivePath, ssts)
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Close()

	_, err = ca.Get(50)
	if err == nil {
		t.Error("expected error for out-of-range offset")
	}
}

func TestColdArchiveCreatedAtNoFile(t *testing.T) {
	ca := &ColdArchive{path: "/tmp/fake"}
	if !ca.CreatedAt().IsZero() {
		t.Error("CreatedAt should be zero when file is nil")
	}
}
