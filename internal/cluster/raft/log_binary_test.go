package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeBinaryEmpty(t *testing.T) {
	data := encodeLogBinary(1, nil)
	fi, entries, err := decodeLogBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	if fi != 1 {
		t.Errorf("firstIndex = %d, want 1", fi)
	}
	if len(entries) != 0 {
		t.Errorf("entries = %d, want 0", len(entries))
	}
}

func TestEncodeDecodeBinaryEntries(t *testing.T) {
	entries := []LogEntry{
		{Index: 1, Term: 1, Type: EntryCommand, Data: []byte("hello")},
		{Index: 2, Term: 1, Type: EntryNoOp, Data: nil},
		{Index: 3, Term: 2, Type: EntryConfigChange, Data: []byte(`{"node":"n2"}`)},
	}

	data := encodeLogBinary(1, entries)
	fi, decoded, err := decodeLogBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	if fi != 1 {
		t.Errorf("firstIndex = %d, want 1", fi)
	}
	if len(decoded) != 3 {
		t.Fatalf("entries = %d, want 3", len(decoded))
	}

	for i, e := range decoded {
		if e.Index != entries[i].Index {
			t.Errorf("entry[%d].Index = %d, want %d", i, e.Index, entries[i].Index)
		}
		if e.Term != entries[i].Term {
			t.Errorf("entry[%d].Term = %d, want %d", i, e.Term, entries[i].Term)
		}
		if e.Type != entries[i].Type {
			t.Errorf("entry[%d].Type = %d, want %d", i, e.Type, entries[i].Type)
		}
		if string(e.Data) != string(entries[i].Data) {
			t.Errorf("entry[%d].Data = %q, want %q", i, e.Data, entries[i].Data)
		}
	}
}

func TestDecodeBinaryInvalidMagic(t *testing.T) {
	data := make([]byte, logHeaderSize)
	_, _, err := decodeLogBinary(data)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestDecodeBinaryTruncated(t *testing.T) {
	data := make([]byte, 10) // too short
	_, _, err := decodeLogBinary(data)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestDecodeBinaryVersionMismatch(t *testing.T) {
	data := make([]byte, logHeaderSize)
	// Set magic correctly but version to 999
	copy(data, []byte{0x52, 0x41, 0x4C, 0x54}) // magic
	data[4] = 0x03
	data[5] = 0xE7 // version 999
	_, _, err := decodeLogBinary(data)
	if err == nil {
		t.Error("expected error for version mismatch")
	}
}

func TestRaftLogSaveLoadBinary(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	log.Append(
		LogEntry{Index: 1, Term: 1, Type: EntryCommand, Data: []byte("cmd1")},
		LogEntry{Index: 2, Term: 1, Type: EntryCommand, Data: []byte("cmd2")},
	)

	if err := log.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify binary file was created
	if _, err := os.Stat(filepath.Join(dir, "log.bin")); os.IsNotExist(err) {
		t.Error("expected log.bin to exist")
	}

	// Load into a new log
	log2 := NewRaftLog(dir)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if log2.Len() != 2 {
		t.Fatalf("Len = %d, want 2", log2.Len())
	}

	e := log2.Get(1)
	if e == nil || string(e.Data) != "cmd1" {
		t.Errorf("entry[1] = %v", e)
	}

	e2 := log2.Get(2)
	if e2 == nil || string(e2.Data) != "cmd2" {
		t.Errorf("entry[2] = %v", e2)
	}
}

func TestRaftLogBinaryMigrationFromJSON(t *testing.T) {
	dir := t.TempDir()

	// Write a legacy JSON log file
	jsonData := `{"entries":[{"Index":1,"Term":1,"Type":0,"Data":"aGVsbG8="}],"first_index":1}`
	jsonPath := filepath.Join(dir, "log.json")
	if err := os.WriteFile(jsonPath, []byte(jsonData), 0644); err != nil {
		t.Fatal(err)
	}

	// Load should fall back to JSON
	log := NewRaftLog(dir)
	if err := log.Load(); err != nil {
		t.Fatalf("Load from JSON: %v", err)
	}

	if log.Len() != 1 {
		t.Fatalf("Len = %d, want 1", log.Len())
	}

	e := log.Get(1)
	if e == nil {
		t.Fatal("entry 1 not found")
	}
	if e.Term != 1 {
		t.Errorf("term = %d, want 1", e.Term)
	}

	// Save should now write binary format
	if err := log.Save(); err != nil {
		t.Fatalf("Save binary: %v", err)
	}

	// Binary file should exist
	if _, err := os.Stat(filepath.Join(dir, "log.bin")); os.IsNotExist(err) {
		t.Error("expected log.bin after migration save")
	}
}

func TestBinaryFormatSizeReduction(t *testing.T) {
	entries := make([]LogEntry, 100)
	for i := range entries {
		entries[i] = LogEntry{
			Index: Index(i + 1),
			Term:  1,
			Type:  EntryCommand,
			Data:  []byte("some-command-data-here-32-bytes!"),
		}
	}

	binSize := len(encodeLogBinary(1, entries))

	// Each entry is ~32 bytes data + 24 bytes fixed = 56 bytes
	// 100 entries = 5600 + 18 header = 5618 bytes
	if binSize > 6000 {
		t.Errorf("binary format too large: %d bytes for 100 entries", binSize)
	}

	// Sanity check: should be much smaller than JSON equivalent
	// JSON with base64 data would be roughly 2x+ larger
	if binSize == 0 {
		t.Error("binary data should not be empty")
	}
}

func TestRaftLogSaveLoadWithCompaction(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	for i := 1; i <= 10; i++ {
		log.Append(LogEntry{Index: Index(i), Term: 1, Type: EntryCommand, Data: []byte("d")})
	}

	log.Compact(5) // Remove entries 1-5
	if err := log.Save(); err != nil {
		t.Fatalf("Save after compact: %v", err)
	}

	log2 := NewRaftLog(dir)
	if err := log2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if log2.Len() != 5 {
		t.Fatalf("Len = %d, want 5", log2.Len())
	}

	// First entry should be at index 6
	e := log2.Get(6)
	if e == nil {
		t.Fatal("entry 6 not found")
	}
}
