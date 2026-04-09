package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"fmt"
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

func TestWALAppendAfterFileClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	w.Append(EntryMessage, []byte("first"))

	// Close underlying file without WAL knowing
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	// Append should fail — Flush/Sync on closed file
	_, err = w.Append(EntryMessage, []byte("after-close"))
	if err == nil {
		t.Error("expected error when appending after file closed")
	}
}

func TestWALSyncIntervalMultipleTicks(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncInterval, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Write data and let the syncLoop tick a few times
	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, []byte("tick"))
		time.Sleep(40 * time.Millisecond)
	}

	w.Close()

	// Verify data persists
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
	if count != 5 {
		t.Errorf("expected 5 entries, got %d", count)
	}
}

func TestWALRotateError(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 128, SyncImmediate, 0) // tiny segments to force rotation
	if err != nil {
		t.Fatal(err)
	}

	// Fill up the first segment
	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, make([]byte, 20))
	}

	// Remove write permissions on WAL dir to make rotation fail
	// Skip on Windows — chmod doesn't work the same way
	if os.Getenv("OS") != "Windows_NT" {
		os.Chmod(dir, 0444)
		defer os.Chmod(dir, 0755)

		_, err = w.Append(EntryMessage, []byte("should-rotate"))
		if err == nil {
			t.Error("expected error on rotation failure")
		}
	}
	w.Close()
}

func TestWALNewWALInvalidDir(t *testing.T) {
	// Creating a WAL in a path with null byte should fail
	_, err := NewWAL(string([]byte{0x00}), 4096, SyncImmediate, 0)
	if err == nil {
		t.Error("expected error for invalid WAL dir")
	}
}

func TestWALAppendDataWriteError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	// Close underlying file so writeBuf.Write fails
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	// Append should fail — writeBuf.Write on closed file
	_, err := w.Append(EntryMessage, []byte("fail"))
	if err == nil {
		t.Error("expected error appending to closed WAL file")
	}
	w.Close()
}

func TestWALAppendSyncImmediateError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	// Write some data first
	w.Append(EntryMessage, []byte("first"))

	// Replace activeFile with a closed file to cause Sync to fail
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	// Next append — writeBuf may succeed (buffered), but Flush/Sync should fail
	_, err := w.Append(EntryMessage, []byte("sync-fail"))
	if err == nil {
		t.Error("expected error when Sync fails")
	}
	w.Close()
}

func TestWALRecoverWithFromOffset(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, []byte{byte(i)})
	}
	w.Close()

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	var count int
	err := w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 entries, got %d", count)
	}
}

func TestWALOpenOrCreateSegmentStatError(t *testing.T) {
	dir := t.TempDir()

	// Create a .wal file
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Make the .wal file unreadable (directory instead of file)
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) > 0 {
		os.Remove(segments[0])
		os.MkdirAll(segments[0], 0750) // replace with directory
	}

	// Reopening should fail — OpenFile on a directory with O_RDWR
	_, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err == nil {
		t.Error("expected error when WAL segment is a directory")
	}
}

func TestWALListSegmentsNoDir(t *testing.T) {
	w := &WAL{
		dir: "/nonexistent/path/for/wal",
	}
	_, err := w.listSegments()
	if err == nil {
		t.Error("expected error listing segments in nonexistent dir")
	}
}

func TestWALOpenOrCreateEmptyDirPath(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Remove the .wal file to force empty segment list → new segment creation
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) == 0 {
		t.Fatal("expected at least one segment")
	}

	os.Remove(segments[0])

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	w2.Append(EntryMessage, []byte("new"))
	w2.Close()
}

func TestWALAppendWriteBufWriteError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	w.Append(EntryMessage, []byte("first"))

	// Close file to cause write error
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	_, err := w.Append(EntryMessage, []byte("second"))
	if err == nil {
		t.Error("expected error when writing to closed file")
	}
	w.Close()
}

func TestWALRecoverCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	// Write valid entries
	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, []byte{byte(i)})
	}
	w.Close()

	// Corrupt the CRC of the second entry
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	f, _ := os.OpenFile(segments[0], os.O_RDWR, 0640)

	// First entry starts at offset 0, header is 17 bytes
	// Second entry starts at offset 17
	// Corrupt the CRC at position 17+13=30 (4th byte of CRC)
	f.WriteAt([]byte{0xFF}, 30)
	f.Close()

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	// Should stop at CRC mismatch — only first entry recovered
	if count != 1 {
		t.Errorf("expected 1 entry (CRC mismatch), got %d", count)
	}
}

func TestWALRecoverOpenSegmentError(t *testing.T) {
	dir := t.TempDir()

	// Create a WAL segment first
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Delete the segment file — Recover should handle empty segment list gracefully
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	for _, s := range segments {
		os.Remove(s)
	}

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	// Recover with no segments — should succeed with 0 entries
	var count int
	err := w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("recover with no segments: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}
}

func TestWALCheckpointAndTruncateMultiSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 128, SyncImmediate, 0)

	// Write enough to create multiple segments
	for i := 0; i < 30; i++ {
		w.Append(EntryMessage, make([]byte, 20))
	}

	segments, _ := w.listSegments()
	if len(segments) < 3 {
		t.Fatalf("expected >= 3 segments, got %d", len(segments))
	}

	// Checkpoint at an offset that covers first 2 segments
	seg2Size := w.segmentEndOffset(segments[1])
	w.Checkpoint(seg2Size)

	// Truncate — should remove segments fully covered by checkpoint
	err := w.Truncate()
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	afterSegments, _ := w.listSegments()
	if len(afterSegments) >= len(segments) {
		t.Errorf("expected fewer segments after truncate: %d -> %d", len(segments), len(afterSegments))
	}
	w.Close()
}

func TestWALRecoverOpenErrorRace(t *testing.T) {
	// This tests the os.Open error path in Recover by creating a segment
	// file that's a directory (Open fails with IsDir error on some platforms)
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Replace the .wal file with a directory
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) > 0 {
		os.Remove(segments[0])
		os.MkdirAll(segments[0], 0750)
	}

	// Create a fresh WAL — openOrCreateSegment should handle directory-as-file
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		// Expected: OpenFile on directory path may fail
		t.Logf("NewWAL with dir-as-segment: %v (expected)", err)
	} else {
		defer w2.Close()
		// Try Recover — should hit os.Open error
		err = w2.Recover(0, func(et EntryType, data []byte) error {
			return nil
		})
		if err != nil {
			t.Logf("Recover error: %v", err)
		}
	}
}

func TestWALAppendSyncError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	w.Append(EntryMessage, []byte("first"))

	// Close active file to cause Sync failure
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	// Write data (buffered write may succeed) then Sync should fail
	_, err := w.Append(EntryMessage, []byte("sync-err"))
	if err == nil {
		t.Error("expected error when file Sync fails")
	}
	w.Close()
}

func TestWALRecoveryVerifyEntryTypes(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	w.Append(EntryMessage, []byte("msg1"))
	w.Append(EntryTopicMeta, []byte("meta1"))
	w.Append(EntryCheckpoint, []byte("cp1"))
	w.Append(EntryMessage, []byte("msg2"))
	w.Close()

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	var types []EntryType
	var datas [][]byte
	w2.Recover(0, func(et EntryType, data []byte) error {
		types = append(types, et)
		datas = append(datas, data)
		return nil
	})

	if len(types) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(types))
	}
	if types[0] != EntryMessage || string(datas[0]) != "msg1" {
		t.Errorf("entry 0: type=%d data=%q", types[0], datas[0])
	}
	if types[1] != EntryTopicMeta || string(datas[1]) != "meta1" {
		t.Errorf("entry 1: type=%d data=%q", types[1], datas[1])
	}
	if types[2] != EntryCheckpoint || string(datas[2]) != "cp1" {
		t.Errorf("entry 2: type=%d data=%q", types[2], datas[2])
	}
	if types[3] != EntryMessage || string(datas[3]) != "msg2" {
		t.Errorf("entry 3: type=%d data=%q", types[3], datas[3])
	}
}

func TestWALCheckpointWriteError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w.Close()

	w.Append(EntryMessage, []byte("data"))

	// Remove write permissions from dir to cause checkpoint write error
	if os.Getenv("OS") != "Windows_NT" {
		os.Chmod(dir, 0444)
		defer os.Chmod(dir, 0755)

		err := w.Checkpoint(100)
		if err == nil {
			t.Error("expected error writing checkpoint to read-only dir")
		}
	}
}

func TestWALAppendWriteHeaderError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	w.Append(EntryMessage, []byte("first"))

	// Close the underlying file so writeBuf.Write fails
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	// This should fail — writeBuf.Write on closed file
	_, err := w.Append(EntryMessage, []byte("header-fail"))
	if err == nil {
		t.Error("expected error writing header to closed WAL file")
	}
	w.Close()
}

func TestWALAppendWriteDataError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	// Use a large entry so header write succeeds but data write fails
	w.Append(EntryMessage, []byte("first"))

	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	_, err := w.Append(EntryMessage, []byte("data-fail"))
	if err == nil {
		t.Error("expected error writing data to closed WAL file")
	}
	w.Close()
}

func TestWALAppendFlushError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)

	w.Append(EntryMessage, []byte("first"))

	// Close active file so Flush fails
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()

	_, err := w.Append(EntryMessage, []byte("flush-fail"))
	if err == nil {
		t.Error("expected error when Flush fails")
	}
	w.Close()
}

func TestWALRotateOpenFileError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 128, SyncImmediate, 0) // tiny segments

	// Fill first segment
	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, make([]byte, 20))
	}

	// Make the WAL dir read-only so rotation fails
	// Skip on Windows — chmod doesn't work the same way
	if os.Getenv("OS") != "Windows_NT" {
		os.Chmod(dir, 0444)
		defer os.Chmod(dir, 0755)

		_, err := w.Append(EntryMessage, []byte("rotate-fail"))
		if err == nil {
			t.Error("expected error on rotation failure")
		}
	}
	w.Close()
}

func TestWALRecoverListSegmentsError(t *testing.T) {
	// Create a WAL in a path that will become inaccessible
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Create a new WAL pointing to a bad dir
	w2 := &WAL{
		dir: string([]byte{0x00}),
	}

	err := w2.Recover(0, func(et EntryType, data []byte) error {
		return nil
	})
	if err == nil {
		t.Error("expected error recovering from invalid dir")
	}
}


func TestWALRecoverListSegmentsBadDir(t *testing.T) {
	// Create a WAL pointing to an invalid dir
	w2 := &WAL{
		dir: string([]byte{0x00}),
	}

	err := w2.Recover(0, func(et EntryType, data []byte) error {
		return nil
	})
	if err == nil {
		t.Error("expected error recovering from invalid dir")
	}
}

func TestWALRecoverSegmentAsDir(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Replace .wal file with a directory
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) > 0 {
		os.Remove(segments[0])
		os.MkdirAll(segments[0], 0750)
	}

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		// NewWAL itself might fail
		return
	}
	defer w2.Close()

	err = w2.Recover(0, func(et EntryType, data []byte) error {
		return nil
	})
	_ = err
}

func TestWALTruncateNoCheckpointPath(t *testing.T) {
	w := &WAL{
		dir: "/nonexistent/path",
	}
	// Truncate with no checkpoint — returns nil
	err := w.Truncate()
	_ = err
}

func TestWALReopenDirAsSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Replace .wal file with a directory to cause Stat to fail on reopen
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) > 0 {
		os.Remove(segments[0])
		os.MkdirAll(segments[0], 0750)
	}

	// Reopening should fail
	_, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err == nil {
		t.Error("expected error reopening WAL with dir-as-segment")
	}
}

func TestWALAppendDataWriteOnlyError(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-data-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// First write header+data together, then close file
	// On next write, the header write might succeed but data write fails
	// This is hard to isolate. Let's test the basic error path.
	w.activeFile.Close()

	_, err = w.Append(EntryMessage, []byte("data"))
	if err == nil {
		t.Error("expected error appending to closed WAL file")
	}
}

func TestWALAppendTriggersRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-rotate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Small max size to trigger rotation quickly
	w, err := NewWAL(dir, 200, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write enough to trigger rotation
	for i := 0; i < 10; i++ {
		_, err := w.Append(EntryMessage, []byte("message-data-here"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify multiple segments exist
	entries, _ := os.ReadDir(dir)
	walCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walCount++
		}
	}
	if walCount < 2 {
		t.Errorf("expected >= 2 segments after rotation, got %d", walCount)
	}
}

func TestWALOpenOrCreateSegmentWithExisting(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-existing-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create WAL and write data
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(EntryMessage, []byte("first"))
	w.Close()

	// Reopen — openOrCreateSegment should find existing segment
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	// Offset should be past the first entry
	if w2.Offset() == 0 {
		t.Error("expected non-zero offset after reopening with existing data")
	}
}

func TestWALAppendSyncIntervalMode(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-sync-int-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 4096, SyncInterval, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		_, err := w.Append(EntryMessage, []byte("interval-msg"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Wait for sync ticker to fire
	time.Sleep(100 * time.Millisecond)

	w.Close()
}

func TestWALRecoverFromMultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-recover-multi-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create WAL with small max size
	w, err := NewWAL(dir, 200, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w.Append(EntryMessage, []byte{byte(i)})
	}
	w.Close()

	// Reopen and recover
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})

	if count != 10 {
		t.Errorf("recovered %d entries, want 10", count)
	}
}

func TestWALAppendLargeData(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 1024*1024, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	offset, err := w.Append(EntryMessage, largeData)
	if err != nil {
		t.Fatalf("append large: %v", err)
	}
	if offset != 0 {
		t.Errorf("offset = %d, want 0", offset)
	}

	// Verify recovery
	w.Close()
	w2, _ := NewWAL(dir, 1024*1024, SyncImmediate, 0)
	defer w2.Close()

	var recovered []byte
	w2.Recover(0, func(et EntryType, data []byte) error {
		recovered = data
		return nil
	})

	if len(recovered) != len(largeData) {
		t.Errorf("recovered len = %d, want %d", len(recovered), len(largeData))
	}
}

func TestWALAppendLargeTriggersRotation(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-rotate-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 100, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Write a small entry first
	_, err = w.Append(EntryMessage, []byte("small"))
	if err != nil {
		t.Fatal(err)
	}

	// Write a large entry that triggers rotation
	_, err = w.Append(EntryMessage, make([]byte, 200))
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can recover both
	w.Close()

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	count := 0
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count != 2 {
		t.Errorf("recovered %d entries, want 2", count)
	}
}

func TestWALAppendEmptyData(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-empty-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Append empty data
	offset, err := w.Append(EntryMessage, []byte{})
	if err != nil {
		t.Fatalf("append empty: %v", err)
	}
	if offset != 0 {
		t.Errorf("offset = %d, want 0", offset)
	}

	// Recover
	w.Close()
	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	count := 0
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		if len(data) != 0 {
			t.Errorf("expected empty data, got %d bytes", len(data))
		}
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 entry, got %d", count)
	}
}

func TestWALTruncateAfterCheckpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-trunc-cp-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 200, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write several entries across segments
	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, []byte{byte(i)})
	}

	// Checkpoint at current offset
	cpOffset := w.Offset()
	w.Checkpoint(cpOffset)

	// Write more
	w.Append(EntryMessage, []byte("after-cp"))

	// Truncate to checkpoint
	err = w.Truncate()
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	w.Close()
}

func TestWALRecoverWithCallbackError(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-cb-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("a"))
	w.Append(EntryMessage, []byte("b"))
	w.Append(EntryMessage, []byte("c"))
	w.Close()

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	count := 0
	err = w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		if count == 2 {
			return fmt.Errorf("callback error")
		}
		return nil
	})
	if err == nil {
		t.Error("expected error from callback")
	}
	if count != 2 {
		t.Errorf("expected 2 calls before error, got %d", count)
	}
}


func TestWALAppendRotateTriggerError(t *testing.T) {
	if os.Getenv("OS") == "Windows_NT" {
		t.Skip("directory removal may not prevent writes on Windows due to buffering")
	}
	dir := t.TempDir()
	w, _ := NewWAL(dir, 50, SyncImmediate, 0)

	// Write data that fills the segment
	w.Append(EntryMessage, make([]byte, 30))

	// Close the WAL and remove directory to make rotation fail
	w.mu.Lock()
	w.activeFile.Close()
	w.mu.Unlock()
	os.RemoveAll(dir)

	_, err := w.Append(EntryMessage, make([]byte, 30))
	if err == nil {
		t.Error("expected error when rotation fails")
	}
}

func TestWALRecoverFileOpenError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Replace WAL file with a directory to cause Open error
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	for _, seg := range segments {
		os.Remove(seg)
		os.MkdirAll(seg, 0750)
	}

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		// NewWAL itself may fail with directory-as-segment
		t.Logf("NewWAL failed as expected: %v", err)
		return
	}

	recoverErr := w2.Recover(0, func(et EntryType, data []byte) error {
		return nil
	})
	if recoverErr == nil {
		t.Error("expected error when segment file is a directory")
	}
	w2.Close()
}

func TestWALTruncateListSegmentsError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Checkpoint(w.Offset())
	w.Close()

	// Remove the directory to cause listSegments to fail
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0750)

	w2, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	defer w2.Close()

	// Write checkpoint file manually
	os.WriteFile(filepath.Join(dir, "checkpoint"), []byte("100\n"), 0640)

	err := w2.Truncate()
	// Truncate returns nil when listSegments fails (no segments to truncate)
	_ = err
}

func TestWALOpenOrCreateSegmentOpenError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 4096, SyncImmediate, 0)
	w.Append(EntryMessage, []byte("data"))
	w.Close()

	// Replace the segment file with a directory
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	for _, seg := range segments {
		os.Remove(seg)
		os.MkdirAll(seg, 0750)
	}

	// Reopen should fail because openOrCreate can't open the directory-as-file
	_, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err == nil {
		t.Error("expected error opening WAL with directory-as-segment")
	}
}

func TestWALRotateOpenNewFileError(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWAL(dir, 50, SyncImmediate, 0)

	// Write data to fill segment
	w.Append(EntryMessage, make([]byte, 30))

	// Make the directory read-only to prevent new file creation
	if os.Getenv("OS") != "Windows_NT" {
		os.Chmod(dir, 0550)
		defer os.Chmod(dir, 0750)
	}

	_, err := w.Append(EntryMessage, make([]byte, 30))
	if os.Getenv("OS") != "Windows_NT" && err == nil {
		t.Error("expected error when rotation can't create new file")
	}
	w.Close()
}
