package warm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewMemTableDefaultCapacity(t *testing.T) {
	mt := NewMemTable(0)
	if mt.capacity != 4*1024*1024 {
		t.Errorf("capacity = %d, want %d", mt.capacity, 4*1024*1024)
	}

	mtNeg := NewMemTable(-1)
	if mtNeg.capacity != 4*1024*1024 {
		t.Errorf("capacity for negative = %d, want %d", mtNeg.capacity, 4*1024*1024)
	}
}

func TestMemTableIteratorEntryOutOfBounds(t *testing.T) {
	it := &MemTableIterator{entries: []MemTableEntry{{Key: []byte("k")}}, pos: -1}
	if e := it.Entry(); e.Key != nil {
		t.Errorf("Entry at pos -1 should be empty, got %+v", e)
	}

	it.pos = 5
	if e := it.Entry(); e.Key != nil {
		t.Errorf("Entry beyond length should be empty, got %+v", e)
	}
}

func TestManifestSaveFailure(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManifest(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Remove the directory so save fails
	os.RemoveAll(dir)

	err = m.save()
	if err == nil {
		t.Error("expected save error when directory is missing")
	}
}

func TestOpenSSTableTooSmall(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.sst")
	os.WriteFile(path, []byte("tooshort"), 0644)

	_, err := OpenSSTable(path)
	if err == nil {
		t.Error("expected error for SSTable smaller than footer")
	}
}

func TestOpenSSTableBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badmagic.sst")

	// Create a file with valid footer size but wrong magic
	footer := make([]byte, sstFooterSize)
	// magic at bytes 0-4 = wrong value
	footer[0] = 0xDE
	footer[1] = 0xAD
	footer[2] = 0xBE
	footer[3] = 0xEF
	os.WriteFile(path, footer, 0644)

	_, err := OpenSSTable(path)
	if err == nil {
		t.Error("expected error for SSTable with bad magic")
	}
}

func TestCompareKeysAllBranches(t *testing.T) {
	tests := []struct {
		a, b   []byte
		expect int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("abc"), []byte("abc"), 0},
		{[]byte("ab"), []byte("abc"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte(""), 1},
		{[]byte(""), []byte(""), 0},
	}

	for _, tt := range tests {
		got := compareKeys(tt.a, tt.b)
		if got != tt.expect {
			t.Errorf("compareKeys(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.expect)
		}
	}
}
