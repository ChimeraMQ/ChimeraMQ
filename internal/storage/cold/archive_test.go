package cold

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/storage/warm"
)

func createTestSSTables(t *testing.T, dir string, ranges []struct{ min, max uint64 }) []*warm.SSTable {
	t.Helper()
	os.MkdirAll(dir, 0755)
	var tables []*warm.SSTable

	for _, r := range ranges {
		mt := warm.NewMemTable(4096)
		for off := r.min; off <= r.max; off++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, off)
			mt.Put(key, []byte(fmt.Sprintf("value-%d", off)))
		}
		mt.Freeze()
		sst, err := warm.FlushMemTable(mt, dir)
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, sst)
		time.Sleep(time.Millisecond) // ensure unique filenames
	}
	return tables
}

func closeSSTables(ssts []*warm.SSTable) {
	for _, s := range ssts {
		s.Close()
	}
}

func TestColdArchiveCreateAndRead(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{0, 49},
		{50, 99},
	})

	archivePath := filepath.Join(dir, "archive-001.dat")
	ca, err := CreateColdArchive(archivePath, ssts)
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Close()
	defer closeSSTables(ssts)

	offRange := ca.OffsetRange()
	if offRange.Min != 0 || offRange.Max != 99 {
		t.Errorf("OffsetRange = [%d,%d], want [0,99]", offRange.Min, offRange.Max)
	}

	for off := uint64(0); off < 100; off++ {
		val, err := ca.Get(off)
		if err != nil {
			t.Errorf("Get(%d) error: %v", off, err)
			continue
		}
		expected := fmt.Sprintf("value-%d", off)
		if string(val) != expected {
			t.Errorf("Get(%d) = %q, want %q", off, val, expected)
		}
	}
}

func TestColdArchiveOutOfRange(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{10, 20},
	})
	defer closeSSTables(ssts)

	archivePath := filepath.Join(dir, "archive.dat")
	ca, _ := CreateColdArchive(archivePath, ssts)
	defer ca.Close()

	_, err := ca.Get(5)
	if err == nil {
		t.Error("should error on out-of-range offset")
	}
}

func TestColdArchiveReopen(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{0, 20},
	})

	archivePath := filepath.Join(dir, "archive.dat")
	ca1, _ := CreateColdArchive(archivePath, ssts)
	ca1.Close()
	closeSSTables(ssts)

	ca2, err := OpenColdArchive(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer ca2.Close()

	val, err := ca2.Get(10)
	if err != nil || string(val) != "value-10" {
		t.Errorf("Get(10) after reopen = (%q, %v)", val, err)
	}
}

func TestColdArchiveSize(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	ssts := createTestSSTables(t, sstDir, []struct{ min, max uint64 }{
		{0, 99},
	})
	defer closeSSTables(ssts)

	archivePath := filepath.Join(dir, "archive.dat")
	ca, _ := CreateColdArchive(archivePath, ssts)
	defer ca.Close()

	if ca.Size() <= 0 {
		t.Error("archive should have positive size")
	}
}

func TestColdArchiveTombstone(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, "sst")
	os.MkdirAll(sstDir, 0755)

	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 10; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("val-%d", i)))
	}
	key5 := make([]byte, 8)
	binary.BigEndian.PutUint64(key5, 5)
	mt.Delete(key5)
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

	val, err := ca.Get(5)
	if err != nil {
		t.Errorf("Get(5) should not error for tombstone: %v", err)
	}
	if val != nil {
		t.Errorf("Get(5) = %q, want nil (tombstone)", val)
	}

	val, err = ca.Get(3)
	if err != nil || string(val) != "val-3" {
		t.Errorf("Get(3) = (%q, %v), want (val-3, nil)", val, err)
	}
}
