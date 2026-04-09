package hot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionReadRange(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write 5 messages
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		if _, err := p.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	// Read full range
	results, err := p.ReadRange(0, 4, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for i, r := range results {
		if len(r) != 1 || r[0] != byte(i) {
			t.Errorf("result[%d] = %v, want [%d]", i, r, i)
		}
	}
}

func TestPartitionReadRangeWithLimit(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 10; i++ {
		p.Append([]byte{byte(i)})
	}

	results, err := p.ReadRange(0, 9, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results (max), got %d", len(results))
	}
}

func TestPartitionReadRangePartial(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 5; i++ {
		p.Append([]byte{byte(i)})
	}

	// Read range that extends beyond available data
	results, err := p.ReadRange(3, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestPartitionReadRangeEmpty(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	results, err := p.ReadRange(0, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty partition, got %d", len(results))
	}
}

func TestPartitionLogStartOffset(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Initial log start offset should be 0
	if off := p.LogStartOffset(); off != 0 {
		t.Errorf("log start = %d, want 0", off)
	}

	p.Append([]byte("a"))
	p.Append([]byte("b"))

	if off := p.LogStartOffset(); off != 0 {
		t.Errorf("log start after append = %d, want 0", off)
	}
}

func TestPartitionReadAt(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Append([]byte("hello"))

	// Read at position 32 (after header)
	data, err := p.ReadAt(SegmentHeaderLen)
	if err != nil {
		t.Fatal(err)
	}
	// First 4 bytes are length prefix
	if len(data) < 4 {
		t.Fatalf("data too short: %v", data)
	}
}

func TestPartitionSegmentRollover(t *testing.T) {
	dir := t.TempDir()
	// Small segment to force rollover
	p, err := OpenPartition(dir, "test", 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write enough messages to trigger segment rollover
	for i := 0; i < 50; i++ {
		data := make([]byte, 8)
		data[0] = byte(i)
		if _, err := p.Append(data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Should have multiple segments
	if count := p.SegmentCount(); count < 2 {
		t.Errorf("expected >= 2 segments after rollover, got %d", count)
	}

	// High watermark should be 49
	if hw := p.HighWatermark(); hw != 49 {
		t.Errorf("high watermark = %d, want 49", hw)
	}

	// Verify we can read from early segments
	data, err := p.Read(0)
	if err != nil {
		t.Fatalf("read offset 0: %v", err)
	}
	if len(data) != 8 || data[0] != 0 {
		t.Errorf("data at 0 = %v, want [0 ...]", data)
	}

	// And from later segments
	data, err = p.Read(49)
	if err != nil {
		t.Fatalf("read offset 49: %v", err)
	}
	if data[0] != 49 {
		t.Errorf("data at 49[0] = %d, want 49", data[0])
	}
}

func TestPartitionSegmentCount(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if count := p.SegmentCount(); count != 1 {
		t.Errorf("initial segments = %d, want 1", count)
	}
}

func TestOpenSegmentRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000000000000000000.log")

	// Create and write data
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	seg.Append([]byte("msg1"))
	seg.Append([]byte("msg2"))
	seg.Append([]byte("msg3"))
	seg.Close()

	// Reopen and verify
	seg2, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	if seg2.NextOffset() != 3 {
		t.Errorf("nextOffset after recovery = %d, want 3", seg2.NextOffset())
	}
	if seg2.Size() == 0 {
		t.Error("size should be > 0 after recovery")
	}

	// Verify we can find and read messages
	pos, err := seg2.FindPosition(0)
	if err != nil {
		t.Fatalf("find position 0: %v", err)
	}
	data, err := seg2.ReadAt(pos)
	if err != nil {
		t.Fatalf("read at position %d: %v", pos, err)
	}
	if string(data) != "msg1" {
		t.Errorf("data = %q, want msg1", string(data))
	}
}

func TestOpenSegmentBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.log")

	// Write invalid magic bytes
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(make([]byte, 64)) // all zeros = invalid magic
	f.Close()

	_, err = OpenSegment(path, 0, 1024*1024)
	if err != ErrBadMagic {
		t.Errorf("expected ErrBadMagic, got %v", err)
	}
}

func TestSegmentSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "size.log")
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	initialSize := seg.Size()
	if initialSize != SegmentHeaderLen {
		t.Errorf("initial size = %d, want %d", initialSize, SegmentHeaderLen)
	}

	seg.Append([]byte("hello"))
	if seg.Size() != initialSize+4+5 { // 4 byte len + 5 byte payload
		t.Errorf("size after append = %d, want %d", seg.Size(), initialSize+4+5)
	}
}

func TestSegmentLoadSaveIndexRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.log")
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Write enough to populate the sparse index
	for i := 0; i < 300; i++ {
		data := []byte{byte(i % 256)}
		seg.Append(data)
	}

	if err := seg.SaveIndex(); err != nil {
		t.Fatalf("save index: %v", err)
	}

	// Load index into a new segment
	seg2, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	if err := seg2.LoadIndex(); err != nil {
		t.Fatalf("load index: %v", err)
	}

	// Verify we can still find offsets
	pos, err := seg2.FindPosition(256)
	if err != nil {
		t.Fatalf("find position 256: %v", err)
	}
	if pos == 0 {
		t.Error("expected non-zero position for offset 256")
	}
}

func TestSegmentFreeze(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "freeze.log")
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	if err := seg.Freeze(); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	// Writing to frozen segment should fail
	_, _, err = seg.Append([]byte("should fail"))
	if err != ErrSegmentReadOnly {
		t.Errorf("expected ErrSegmentReadOnly, got %v", err)
	}
}

func TestEngineFlushAll(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 1024 * 1024})
	defer eng.Close()

	p, err := eng.GetOrCreatePartition("flush-test", 0)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("data"))

	// Should not panic
	eng.FlushAll()
}

func TestEngineCloseEmpty(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 1024 * 1024})

	// Close without any partitions should be fine
	if err := eng.Close(); err != nil {
		t.Errorf("close empty engine: %v", err)
	}
}

func TestEngineGetOrCreatePartition(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 1024 * 1024})
	defer eng.Close()

	p1, err := eng.GetOrCreatePartition("topic1", 0)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := eng.GetOrCreatePartition("topic1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Error("should return same partition instance")
	}

	p3, err := eng.GetOrCreatePartition("topic1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p3 {
		t.Error("different partition IDs should be different instances")
	}

	p4, err := eng.GetOrCreatePartition("topic2", 0)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p4 {
		t.Error("different topics should be different instances")
	}
}

func TestReadRecordAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "record.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write a length-prefixed record manually
	var lenBuf [4]byte
	data := []byte("test-record")
	lenBuf[0] = byte(len(data) >> 24)
	lenBuf[1] = byte(len(data) >> 16)
	lenBuf[2] = byte(len(data) >> 8)
	lenBuf[3] = byte(len(data))
	f.Write(lenBuf[:])
	f.Write(data)
	f.Sync()

	result, err := ReadRecordAt(f, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "test-record" {
		t.Errorf("result = %q, want test-record", string(result))
	}
}

func TestPartitionRecoveryAfterClose(t *testing.T) {
	dir := t.TempDir()

	// Create partition and write data
	p, err := OpenPartition(dir, "recovery", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		p.Append([]byte{byte(i)})
	}
	p.Close()

	// Reopen and verify
	p2, err := OpenPartition(dir, "recovery", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	if hw := p2.HighWatermark(); hw != 4 {
		t.Errorf("high watermark after recovery = %d, want 4", hw)
	}

	data, err := p2.Read(0)
	if err != nil {
		t.Fatalf("read after recovery: %v", err)
	}
	if len(data) != 1 || data[0] != 0 {
		t.Errorf("data[0] = %v, want [0]", data)
	}
}

func TestPartitionReadOffsetNotFound(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Append([]byte("a"))
	p.Append([]byte("b"))

	// Read an offset that doesn't exist (too high)
	_, err = p.Read(99)
	if err == nil {
		t.Error("expected error for non-existent offset")
	}
}

func TestPartitionLoadSegmentsMalformedFile(t *testing.T) {
	dir := t.TempDir()
	// Create a file that looks like a segment but has malformed name
	f, err := os.Create(filepath.Join(dir, "not-a-number.log"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Should skip the malformed filename
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if count := p.SegmentCount(); count != 1 {
		t.Errorf("expected 1 segment (new default), got %d", count)
	}
}

func TestPartitionReadRangeWithSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	// Small segment to force rollover
	p, err := OpenPartition(dir, "test", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 20; i++ {
		p.Append([]byte{byte(i)})
	}

	// Read across segment boundaries
	results, err := p.ReadRange(0, 19, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 20 {
		t.Errorf("expected 20 results across segments, got %d", len(results))
	}
}

func TestSegmentAppendFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "full.log")
	seg, err := OpenSegment(path, 0, 64) // very small
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Write until full
	for {
		_, _, err := seg.Append([]byte("x"))
		if err == ErrSegmentFull {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSegmentFindPositionOffsetTooOld(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "old.log")
	seg, err := OpenSegment(path, 10, 1024*1024) // base offset 10
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	seg.Append([]byte("a"))

	// Find position for offset below base
	_, err = seg.FindPosition(5)
	if err != ErrOffsetTooOld {
		t.Errorf("expected ErrOffsetTooOld, got %v", err)
	}
}

func TestSegmentReadAtInvalidPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badpos.log")
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Read at a position past end
	_, err = seg.ReadAt(999999)
	if err == nil {
		t.Error("expected error for reading past end")
	}
}

func TestReadRecordAtInvalidPosition(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "norecord.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read from empty file
	_, err = ReadRecordAt(f, 0)
	if err == nil {
		t.Error("expected error for reading from empty file")
	}
}

func TestSegmentRebuildIndexWithTrailingPartial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.log")
	seg, _ := OpenSegment(path, 0, 1024*1024)
	seg.Append([]byte("msg1"))
	seg.Append([]byte("msg2"))
	seg.Close()

	// Append a partial length prefix (simulates crash during write)
	f, err := os.OpenFile(path, os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	// Write 3 bytes of a partial length prefix at end
	f.WriteAt([]byte{0, 0, 1}, info.Size())
	f.Close()

	// Reopen — should recover what it can, ignoring partial entry
	seg2, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	if seg2.NextOffset() != 2 {
		t.Errorf("nextOffset = %d, want 2 (partial entry should be skipped)", seg2.NextOffset())
	}
}

func TestEngineFlushAllMultiplePartitions(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 1024 * 1024})
	defer eng.Close()

	p1, _ := eng.GetOrCreatePartition("t1", 0)
	p2, _ := eng.GetOrCreatePartition("t2", 0)
	p1.Append([]byte("a"))
	p2.Append([]byte("b"))

	// Should flush all partitions without error
	eng.FlushAll()
}

func TestPartitionAppendAndReadMany(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 500; i++ {
		data := []byte{byte(i % 256)}
		offset, err := p.Append(data)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("offset = %d, want %d", offset, i)
		}
	}

	// Verify reads
	for i := 0; i < 500; i++ {
		data, err := p.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if len(data) != 1 || data[0] != byte(i%256) {
			t.Errorf("data[%d] = %v", i, data)
		}
	}
}

func TestOpenSegmentWithExistingData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.log")

	seg, _ := OpenSegment(path, 0, 1024*1024)
	for i := 0; i < 10; i++ {
		seg.Append([]byte{byte(i)})
	}
	seg.Freeze()
	seg.SaveIndex()
	seg.Close()

	// Reopen — readHeader + rebuildIndex paths
	seg2, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	if seg2.NextOffset() != 10 {
		t.Errorf("nextOffset = %d, want 10", seg2.NextOffset())
	}
	if seg2.BaseOffset() != 0 {
		t.Errorf("baseOffset = %d, want 0", seg2.BaseOffset())
	}
}

func TestPartitionOpenWithExistingSegments(t *testing.T) {
	dir := t.TempDir()

	// Create partition with data
	p, _ := OpenPartition(dir, "test", 0, 128) // small segments
	for i := 0; i < 50; i++ {
		p.Append([]byte{byte(i)})
	}
	p.Close()

	// Reopen — should load existing segments
	p2, err := OpenPartition(dir, "test", 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	if p2.SegmentCount() < 2 {
		t.Errorf("expected >= 2 segments, got %d", p2.SegmentCount())
	}
	if hw := p2.HighWatermark(); hw != 49 {
		t.Errorf("highWater = %d, want 49", hw)
	}

	// Verify can still append
	offset, err := p2.Append([]byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	if offset != 50 {
		t.Errorf("offset after reopen = %d, want 50", offset)
	}
}

func TestPartitionFindPositionAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenPartition(dir, "test", 0, 64) // small segments

	for i := 0; i < 30; i++ {
		p.Append([]byte{byte(i)})
	}
	p.Close()

	// Reopen and read from different segments
	p2, _ := OpenPartition(dir, "test", 0, 64)
	defer p2.Close()

	// Read from first segment
	d0, err := p2.Read(0)
	if err != nil {
		t.Fatalf("read 0: %v", err)
	}
	if d0[0] != 0 {
		t.Errorf("data[0] = %d, want 0", d0[0])
	}

	// Read from last offset
	d29, err := p2.Read(29)
	if err != nil {
		t.Fatalf("read 29: %v", err)
	}
	if d29[0] != 29 {
		t.Errorf("data[29] = %d, want 29", d29[0])
	}
}

func TestPartitionLoadSegmentsNonLogFiles(t *testing.T) {
	dir := t.TempDir()
	// Create various non-log files that should be skipped
	os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("docs"), 0640)
	os.WriteFile(filepath.Join(dir, "data.idx"), []byte("index"), 0640)
	os.MkdirAll(filepath.Join(dir, "subdir"), 0750)

	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if p.SegmentCount() != 1 {
		t.Errorf("expected 1 segment (default), got %d", p.SegmentCount())
	}
}

func TestPartitionAppendFrozenSegmentRollover(t *testing.T) {
	dir, _ := os.MkdirTemp("", "rollover-*")
	defer os.RemoveAll(dir)

	p, _ := OpenPartition(dir, "test", 0, 80) // small

	// Write enough to fill
	for i := 0; i < 5; i++ {
		p.Append([]byte{byte(i)})
	}
	// Trigger rollover with more data
	for i := 5; i < 10; i++ {
		_, err := p.Append([]byte{byte(i)})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify data across rollover
	for i := 0; i < 10; i++ {
		d, err := p.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if d[0] != byte(i) {
			t.Errorf("data[%d] = %d, want %d", i, d[0], i)
		}
	}
	p.Close()
}

func TestSegmentAppendToReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.log")
	seg, _ := OpenSegment(path, 0, 1024*1024)
	seg.Append([]byte("a"))
	seg.Freeze()

	_, _, err := seg.Append([]byte("b"))
	if err != ErrSegmentReadOnly {
		t.Errorf("expected ErrSegmentReadOnly, got %v", err)
	}
	seg.Close()
}

func TestSegmentBaseOffsetNonZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "based.log")
	seg, _ := OpenSegment(path, 100, 1024*1024)
	defer seg.Close()

	if seg.BaseOffset() != 100 {
		t.Errorf("baseOffset = %d, want 100", seg.BaseOffset())
	}

	offset, _, err := seg.Append([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Errorf("first offset = %d, want 100", offset)
	}
	if seg.NextOffset() != 101 {
		t.Errorf("nextOffset = %d, want 101", seg.NextOffset())
	}
}

func TestSegmentFindPositionLinearScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scan.log")
	seg, _ := OpenSegment(path, 0, 1024*1024)
	defer seg.Close()

	// Write 10 messages — sparse index interval is 256, so all are linear scan
	for i := 0; i < 10; i++ {
		seg.Append([]byte{byte(i)})
	}

	// Find position for offset 5 (must linear scan from header)
	pos, err := seg.FindPosition(5)
	if err != nil {
		t.Fatalf("find position 5: %v", err)
	}
	data, err := seg.ReadAt(pos)
	if err != nil {
		t.Fatalf("read at pos %d: %v", pos, err)
	}
	if data[0] != 5 {
		t.Errorf("data at offset 5 = %d, want 5", data[0])
	}

	// Find position for offset 0
	pos0, err := seg.FindPosition(0)
	if err != nil {
		t.Fatalf("find position 0: %v", err)
	}
	data0, err := seg.ReadAt(pos0)
	if err != nil {
		t.Fatalf("read at pos0: %v", err)
	}
	if data0[0] != 0 {
		t.Errorf("data at offset 0 = %d, want 0", data0[0])
	}
}

func TestSegmentWriteHeaderNewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new.log")
	seg, err := OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// New file should have segment header written
	if seg.Size() != SegmentHeaderLen {
		t.Errorf("size = %d, want %d", seg.Size(), SegmentHeaderLen)
	}
}

func TestSegmentReadHeaderExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.log")
	seg, _ := OpenSegment(path, 42, 1024*1024)
	seg.Append([]byte("test"))
	seg.Close()

	// Reopen — should read header and recover baseOffset=42
	seg2, err := OpenSegment(path, 0, 1024*1024) // baseOffset arg ignored for existing file
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	// The readHeader path should have recovered baseOffset from file
	if seg2.BaseOffset() != 42 {
		t.Errorf("baseOffset after readHeader = %d, want 42", seg2.BaseOffset())
	}
}

func TestSegmentOpenStatError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodir", "segment.log")
	// OpenSegment in a non-existent directory should fail
	_, err := OpenSegment(path, 0, 1024*1024)
	if err == nil {
		t.Error("expected error for non-existent directory")
	}
}

func TestSegmentAppendAndReadSequential(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seq.log")
	seg, _ := OpenSegment(path, 0, 1024*1024)
	defer seg.Close()

	// Write 3 messages of varying sizes
	seg.Append([]byte("short"))
	seg.Append([]byte("medium-length-message"))
	seg.Append(make([]byte, 100))

	// Read each by finding position
	for i := 0; i < 3; i++ {
		pos, err := seg.FindPosition(uint64(i))
		if err != nil {
			t.Fatalf("find position %d: %v", i, err)
		}
		data, err := seg.ReadAt(pos)
		if err != nil {
			t.Fatalf("read at offset %d: %v", i, err)
		}
		if len(data) == 0 {
			t.Errorf("empty data at offset %d", i)
		}
	}
}

func TestPartitionLoadSegmentsBadMagic(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "partition-0")
	os.MkdirAll(partDir, 0750)

	// Create a segment file with invalid magic
	badPath := filepath.Join(partDir, "00000000000000000000.log")
	f, err := os.Create(badPath)
	if err != nil {
		t.Fatal(err)
	}
	// Write 32 bytes of zeros (invalid magic)
	f.Write(make([]byte, 32))
	f.Close()

	// OpenPartition should fail because loadSegments → OpenSegment → readHeader → ErrBadMagic
	_, err = OpenPartition(dir, "test", 0, 1024*1024)
	if err == nil {
		t.Error("expected error for segment with bad magic")
	}
}

func TestPartitionOpenWithExistingHighWatermark(t *testing.T) {
	dir := t.TempDir()

	// Create partition with data
	p, _ := OpenPartition(dir, "test", 0, 1024*1024)
	for i := 0; i < 10; i++ {
		p.Append([]byte{byte(i)})
	}
	hw := p.HighWatermark()
	p.Close()

	// Reopen — high watermark should be restored
	p2, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	if p2.HighWatermark() != hw {
		t.Errorf("highWater = %d, want %d", p2.HighWatermark(), hw)
	}
}
