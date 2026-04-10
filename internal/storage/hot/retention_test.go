package hot

import (
	"fmt"
	"testing"
)

func TestEnforceRetentionMaxSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write enough to create multiple segments
	for i := 0; i < 500; i++ {
		data := make([]byte, 200)
		copy(data, fmt.Appendf(nil, "msg-%d", i))
		p.Append(data)
	}

	initialCount := p.SegmentCount()
	if initialCount < 2 {
		t.Fatalf("need at least 2 segments, got %d", initialCount)
	}

	// Enforce max 1 frozen segment
	removed := EnforceRetention(p, RetentionPolicy{MaxSegments: 1})
	if removed == 0 {
		t.Error("should have removed at least one segment")
	}

	// Check that segment count decreased
	if p.SegmentCount() >= initialCount {
		t.Errorf("SegmentCount = %d, should be < %d", p.SegmentCount(), initialCount)
	}
}

func TestEnforceRetentionMaxSize(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write data
	for i := 0; i < 300; i++ {
		data := make([]byte, 200)
		p.Append(data)
	}

	initialCount := p.SegmentCount()
	if initialCount < 2 {
		t.Fatalf("need at least 2 segments, got %d", initialCount)
	}

	// Enforce max size = 300 bytes (should remove old segments)
	removed := EnforceRetention(p, RetentionPolicy{MaxSize: 300})
	if removed == 0 {
		t.Error("should have removed segments due to size")
	}
}

func TestEnforceRetentionNoPolicy(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 100; i++ {
		p.Append([]byte("data"))
	}

	removed := EnforceRetention(p, RetentionPolicy{})
	if removed != 0 {
		t.Error("no policy should remove nothing")
	}
}

func TestShouldCompact(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	lc := NewLogCompactor(CompactKeyBased, 0)

	// Not enough segments
	if lc.ShouldCompact(p) {
		t.Error("should not compact with single segment")
	}

	// Write enough to create multiple segments
	for i := 0; i < 500; i++ {
		data := make([]byte, 200)
		p.Append(data)
	}

	if p.SegmentCount() < 2 {
		t.Skip("need more segments")
	}

	if !lc.ShouldCompact(p) {
		t.Error("should compact with multiple frozen segments")
	}
}
