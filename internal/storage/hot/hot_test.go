package hot

import (
	"testing"
)

func TestSegmentCreateAndAppend(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegment(dir+"/test.log", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	off1, _, err := seg.Append([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	off2, _, err := seg.Append([]byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if off2 <= off1 {
		t.Errorf("offsets should increase")
	}
}

func TestSegmentReadBack(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegment(dir+"/test.log", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	data := []byte("test-message-data")
	offset, pos, err := seg.Append(data)
	if err != nil {
		t.Fatal(err)
	}

	read, err := seg.ReadAt(pos)
	if err != nil {
		t.Fatal(err)
	}
	if string(read) != string(data) {
		t.Errorf("read mismatch: got %q, want %q", read, data)
	}

	findPos, err := seg.FindPosition(offset)
	if err != nil {
		t.Fatal(err)
	}
	if findPos != pos {
		t.Errorf("FindPosition: got %d, want %d", findPos, pos)
	}
}

func TestSegmentFull(t *testing.T) {
	dir := t.TempDir()
	seg, err := OpenSegment(dir+"/test.log", 0, 64) // header=32, leaves 32 for data
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// First write: 4 + 20 = 24 bytes → 32+24=56 total (fits in 64)
	_, _, err = seg.Append(make([]byte, 20))
	if err != nil {
		t.Fatal(err)
	}
	// Second write: 4 + 20 = 24 bytes → 56+24=80 > 64 (should be full)
	_, _, err = seg.Append(make([]byte, 20))
	if err != ErrSegmentFull {
		t.Errorf("expected ErrSegmentFull, got %v", err)
	}
}

func TestPartitionAppendRead(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test-topic", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	off, err := p.Append([]byte("msg-1"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Errorf("first offset should be 0, got %d", off)
	}

	data, err := p.Read(off)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "msg-1" {
		t.Errorf("read mismatch")
	}
}

func TestPartitionMultiSegment(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test-topic", 0, 256) // Small segments
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	count := 100
	for i := 0; i < count; i++ {
		data := []byte{byte(i)}
		_, err := p.Append(data)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if p.SegmentCount() < 2 {
		t.Errorf("expected multiple segments, got %d", p.SegmentCount())
	}

	// Read all back
	for i := 0; i < count; i++ {
		data, err := p.Read(uint64(i))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if data[0] != byte(i) {
			t.Errorf("data mismatch at %d", i)
		}
	}
}

func TestPartitionRecovery(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test-topic", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		p.Append([]byte{byte(i)})
	}
	p.Close()

	// Reopen
	p2, err := OpenPartition(dir, "test-topic", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	if p2.HighWatermark() != 49 {
		t.Errorf("highWater after recovery: got %d, want 49", p2.HighWatermark())
	}

	data, err := p2.Read(49)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 49 {
		t.Errorf("last message mismatch")
	}
}

func TestEngine(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 1024 * 1024})
	defer eng.Close()

	p1, err := eng.GetOrCreatePartition("topic-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	p1_again, _ := eng.GetOrCreatePartition("topic-a", 0)
	if p1 != p1_again {
		t.Error("expected same partition instance")
	}

	p2, err := eng.GetOrCreatePartition("topic-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Error("different partitions should be different instances")
	}
}

func TestSparseIndexSaveLoad(t *testing.T) {
	dir := t.TempDir()
	si := &SparseIndex{
		entries:  make([]IndexEntry, 0),
		interval: 2,
	}
	for i := uint64(0); i < 100; i++ {
		if i%2 == 0 {
			si.Add(i, uint32(i*10), int64(i))
		}
	}

	err := si.Save(dir + "/test.idx")
	if err != nil {
		t.Fatal(err)
	}

	si2 := &SparseIndex{interval: 2}
	err = si2.Load(dir + "/test.idx")
	if err != nil {
		t.Fatal(err)
	}
	if len(si2.entries) != 50 {
		t.Errorf("expected 50 entries, got %d", len(si2.entries))
	}
}
