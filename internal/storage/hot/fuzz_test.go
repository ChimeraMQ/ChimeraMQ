package hot

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzSegmentAppend verifies that segment append operations handle
// arbitrary data safely without panics or data corruption.
func FuzzSegmentAppend(f *testing.F) {
	// Seed with typical message data
	f.Add([]byte("hello world"))
	f.Add([]byte{})
	f.Add(make([]byte, 100))
	f.Add([]byte(`{"topic":"test","payload":"data"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Cap at 1MB to avoid OOM
		if len(data) > 1048576 {
			data = data[:1048576]
		}

		dir := t.TempDir()
		segPath := filepath.Join(dir, "00000000000000000000.log")

		seg, err := OpenSegment(segPath, 0, 1024*1024) // 1MB max segment
		if err != nil {
			t.Skipf("cannot open segment: %v", err)
		}
		defer seg.Close()
		defer os.Remove(segPath)
		defer os.Remove(segPath[:len(segPath)-4] + ".idx")

		// Append should not panic
		_, position, err := seg.Append(data)
		if err != nil {
			return // expected for oversized data
		}

		// If append succeeded, verify we can read it back
		readData, err := seg.ReadAt(position)
		if err != nil {
			t.Errorf("read failed after successful append at position %d: %v", position, err)
			return
		}
		if len(readData) != len(data) {
			t.Errorf("data length mismatch: got %d, want %d", len(readData), len(data))
		}
	})
}

// FuzzSegmentIndexRoundTrip verifies that index save/load handles
// arbitrary offset sequences safely.
func FuzzSegmentIndexRoundTrip(f *testing.F) {
	f.Add(uint64(0), int64(0))
	f.Add(uint64(1), int64(100))
	f.Add(uint64(1000), int64(999999))

	f.Fuzz(func(t *testing.T, offset uint64, pos int64) {
		dir := t.TempDir()
		segPath := filepath.Join(dir, "00000000000000000000.log")

		seg, err := OpenSegment(segPath, offset, 1024*1024)
		if err != nil {
			t.Skipf("cannot open segment: %v", err)
		}

		// Write some data so we can create an index entry
		data := make([]byte, pos%100+1)
		_, _, err = seg.Append(data)
		if err != nil {
			seg.Close()
			return
		}

		err = seg.SaveIndex()
		if err != nil {
			seg.Close()
			return
		}

		// Reload index
		err = seg.LoadIndex()
		if err != nil {
			seg.Close()
			t.Errorf("load index failed: %v", err)
		}

		seg.Close()
	})
}

// FuzzSparseIndex verifies that sparse index operations handle arbitrary
// input safely.
func FuzzSparseIndex(f *testing.F) {
	f.Add(uint64(0), uint32(0), int64(0))
	f.Add(uint64(10), uint32(100), int64(1000))
	f.Add(uint64(100), uint32(1000), int64(10000))

	f.Fuzz(func(t *testing.T, offset uint64, position uint32, ts int64) {
		idx := &SparseIndex{}
		idx.Add(offset, position, ts)

		// Search should not panic
		_, _ = idx.Search(offset)

		// Len and Entries should not panic
		_ = idx.Len()
		_ = idx.Entries()
	})
}
