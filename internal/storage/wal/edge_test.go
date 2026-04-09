package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWALOffset(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	if w.Offset() != 0 {
		t.Errorf("initial offset = %d, want 0", w.Offset())
	}

	off1, _ := w.Append(EntryMessage, []byte("a"))
	if w.Offset() != off1+WALHeaderSize+1 {
		t.Errorf("offset after 1 append = %d, want %d", w.Offset(), off1+WALHeaderSize+1)
	}

	off2, _ := w.Append(EntryMessage, []byte("bb"))
	if w.Offset() <= off1 {
		t.Errorf("offset should increase: %d -> %d", off1, w.Offset())
	}
	_ = off2
}

func TestWALDoubleClose(t *testing.T) {
	w, _ := newTestWAL(t)

	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Second close should not panic — may return error on nil file
	w.Close()
}

func TestWALRecoverCallbackError(t *testing.T) {
	w, dir := newTestWAL(t)

	for i := 0; i < 10; i++ {
		w.Append(EntryMessage, []byte("data"))
	}
	w.Close()

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count atomic.Int32
	err = w2.Recover(0, func(et EntryType, data []byte) error {
		count.Add(1)
		if count.Load() == 3 {
			return errTestAbort
		}
		return nil
	})
	if err != errTestAbort {
		t.Errorf("expected errTestAbort, got %v", err)
	}
	if count.Load() != 3 {
		t.Errorf("expected 3 calls before abort, got %d", count.Load())
	}
}

var errTestAbort = func() error {
	type e struct{ Msg string }
	return json.Unmarshal([]byte("invalid"), &e{})
}()

func TestWALRecoverEmpty(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	var count int
	err := w.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recover empty: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}
}

func TestWALTruncateNoCheckpoint(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	for i := 0; i < 10; i++ {
		w.Append(EntryMessage, []byte("data"))
	}

	// Truncate without checkpoint — should be a no-op
	err := w.Truncate()
	if err != nil {
		t.Errorf("truncate without checkpoint: %v", err)
	}
}

func TestWALTruncateSingleSegment(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	w.Append(EntryMessage, []byte("data"))
	w.Checkpoint(w.Offset())

	// Only 1 segment — should not remove it
	err := w.Truncate()
	if err != nil {
		t.Errorf("truncate single segment: %v", err)
	}

	segments, _ := w.listSegments()
	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
}

func TestWALMultiSegmentRecovery(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 128, SyncImmediate, 0) // small segments
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		data := []byte{byte(i % 256)}
		_, err := w.Append(EntryMessage, data)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Reopen and recover
	w2, err := NewWAL(dir, 128, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count atomic.Int32
	err = w2.Recover(0, func(et EntryType, data []byte) error {
		if len(data) != 1 || data[0] != byte(count.Load()%256) {
			t.Errorf("entry %d: data = %v", count.Load(), data)
		}
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 100 {
		t.Errorf("expected 100 entries, got %d", count.Load())
	}
}

func TestWALSegmentPath(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	path := w.segmentPath(42)
	expected := filepath.Join(w.dir, "000000000042.wal")
	if path != expected {
		t.Errorf("segmentPath(42) = %q, want %q", path, expected)
	}
}

func TestWALSyncOS(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncOS, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append(EntryMessage, []byte("os-sync"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Verify data persisted
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 entry, got %d", count)
	}
}

func TestWALEmptyData(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	off, err := w.Append(EntryMessage, []byte{})
	if err != nil {
		t.Fatalf("append empty: %v", err)
	}
	if off != 0 {
		t.Errorf("offset = %d, want 0", off)
	}

	w2, _ := newTestWAL(t)
	defer w2.Close()
}

func TestWALMultipleEntryTypes(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	w.Append(EntryMessage, []byte("msg"))
	w.Append(EntryTopicMeta, []byte("meta"))
	w.Append(EntryCheckpoint, []byte("cp"))
	w.Close()

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var types []EntryType
	w2.Recover(0, func(et EntryType, data []byte) error {
		types = append(types, et)
		return nil
	})

	if len(types) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(types))
	}
	if types[0] != EntryMessage {
		t.Errorf("type[0] = %d, want EntryMessage", types[0])
	}
	if types[1] != EntryTopicMeta {
		t.Errorf("type[1] = %d, want EntryTopicMeta", types[1])
	}
	if types[2] != EntryCheckpoint {
		t.Errorf("type[2] = %d, want EntryCheckpoint", types[2])
	}
}

func TestWALTruncateKeepsLastSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 128, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough to create multiple segments
	for i := 0; i < 50; i++ {
		w.Append(EntryMessage, make([]byte, 30))
	}
	segments, _ := w.listSegments()
	if len(segments) < 3 {
		t.Fatalf("expected >= 3 segments, got %d", len(segments))
	}

	// Checkpoint at an offset that only covers the first segment
	// The first segment's approximate end offset is its file size
	firstSegSize := w.segmentEndOffset(segments[0])
	w.Checkpoint(firstSegSize)
	w.Truncate()

	segmentsAfter, _ := w.listSegments()
	// Should keep the last segment (the active one) even if earlier ones are removed
	if len(segmentsAfter) == 0 {
		t.Error("expected at least 1 segment after truncate")
	}
	w.Close()
}

func TestWALSegmentEndOffsetNonexistent(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// segmentEndOffset on nonexistent file should return 0
	off := w.segmentEndOffset("/nonexistent/path.wal")
	if off != 0 {
		t.Errorf("expected 0 for nonexistent file, got %d", off)
	}
}

func TestWALNewWALMkdirAll(t *testing.T) {
	dir := t.TempDir()
	nestedDir := dir + "/wal/sub/deep"
	w, err := NewWAL(nestedDir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatalf("NewWAL with nested dirs: %v", err)
	}
	w.Append(EntryMessage, []byte("test"))
	w.Close()
}

func TestWALRecoverMultiSegmentWithCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 128, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		w.Append(EntryMessage, []byte{byte(i)})
	}
	w.Close()

	// Corrupt the second segment
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) < 2 {
		t.Fatal("expected >= 2 segments")
	}
	info, _ := os.Stat(segments[1])
	f, _ := os.OpenFile(segments[1], os.O_RDWR, 0640)
	f.WriteAt([]byte{0xFF}, info.Size()-3)
	f.Close()

	// Recover should get entries from first segment and stop at corruption in second
	w2, err := NewWAL(dir, 128, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count == 0 {
		t.Error("expected at least some entries")
	}
}

func TestWALCloseWithSyncTicker(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncInterval, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(EntryMessage, []byte("data"))
	time.Sleep(80 * time.Millisecond)

	// First close
	w.Close()

	// Double close should not panic — syncTicker path already closed
	w.Close()
}

func TestWALReadCheckpointInvalid(t *testing.T) {
	dir := t.TempDir()
	// No checkpoint file exists
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Truncate when no checkpoint exists — should be no-op
	if err := w.Truncate(); err != nil {
		t.Errorf("truncate without checkpoint: %v", err)
	}
}

func TestWALRecoveryPreservesOffset(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		w.Append(EntryMessage, []byte("data"))
	}
	finalOffset := w.Offset()
	w.Close()

	// Reopen — offset should be restored from segment size
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if w2.Offset() != finalOffset {
		t.Errorf("offset after recovery = %d, want %d", w2.Offset(), finalOffset)
	}
}

func TestWALOpenWithMalformedSegmentName(t *testing.T) {
	dir := t.TempDir()

	// Create a .wal file with a non-numeric name
	f, err := os.Create(filepath.Join(dir, "badname.wal"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// NewWAL should fail to parse the segment name
	_, err = NewWAL(dir, 4096, SyncImmediate, 0)
	if err == nil {
		t.Error("expected error for malformed WAL segment filename")
	}
}

func TestWALListSegmentsSkipsNonWalFiles(t *testing.T) {
	dir := t.TempDir()
	// Create various non-.wal files
	os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("docs"), 0640)
	os.WriteFile(filepath.Join(dir, "checkpoint"), []byte("123"), 0640)

	// Should create a new segment (no existing .wal files)
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	segments, _ := w.listSegments()
	if len(segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(segments))
	}
}

func TestWALReadCheckpointWithValidData(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w.Close()

	w.Append(EntryMessage, []byte("data"))
	offset := w.Offset()
	w.Checkpoint(offset)

	// Read checkpoint via internal method
	cpOffset, err := w.readCheckpoint()
	if err != nil {
		t.Fatalf("readCheckpoint: %v", err)
	}
	if cpOffset != offset {
		t.Errorf("checkpoint offset = %d, want %d", cpOffset, offset)
	}
}
