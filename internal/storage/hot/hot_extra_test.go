package hot

import (
	"testing"
)

func TestEngineTotalSize(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine(dir, HotConfig{SegmentSize: 256})
	defer eng.Close()

	if eng.TotalSize() != 0 {
		t.Errorf("empty engine total size = %d, want 0", eng.TotalSize())
	}

	p1, _ := eng.GetOrCreatePartition("topic-a", 0)
	for i := 0; i < 10; i++ {
		p1.Append([]byte{byte(i)})
	}

	p2, _ := eng.GetOrCreatePartition("topic-b", 0)
	for i := 0; i < 10; i++ {
		p2.Append([]byte{byte(i)})
	}

	size := eng.TotalSize()
	if size <= 0 {
		t.Error("TotalSize should be positive after appending data")
	}
}

func TestPartitionTotalSize(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if p.TotalSize() <= 0 {
		t.Error("new partition should have positive size from header")
	}

	for i := 0; i < 20; i++ {
		p.Append([]byte{byte(i)})
	}

	size := p.TotalSize()
	if size <= 0 {
		t.Error("TotalSize should be positive after appending data")
	}
}
