package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// FuzzWALAppend verifies that WAL append handles arbitrary data safely
// without panics or corruption.
func FuzzWALAppend(f *testing.F) {
	f.Add([]byte("hello wal"))
	f.Add([]byte{})
	f.Add([]byte(`{"topic":"test","partition":0}`))
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1048576 {
			data = data[:1048576]
		}

		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}
		defer w.Close()

		_, err = w.Append(EntryMessage, data)
		if err != nil {
			return // expected for oversized data
		}

		// Verify offset advanced
		if w.Offset() < 1 {
			t.Error("expected offset >= 1 after append")
		}
	})
}

// FuzzWALRecover verifies that WAL recovery handles arbitrary log entries
// safely.
func FuzzWALRecover(f *testing.F) {
	f.Add([]byte("replay me"))
	f.Add([]byte{})
	f.Add(make([]byte, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}

		// Append some data
		_, err = w.Append(EntryMessage, data)
		if err != nil {
			w.Close()
			return
		}
		w.Close()

		// Re-open and recover
		w2, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			return
		}
		defer w2.Close()

		count := 0
		err = w2.Recover(0, func(_ EntryType, _ []byte) error {
			count++
			return nil
		})
		if err != nil {
			t.Errorf("recovery failed: %v", err)
		}
	})
}

// FuzzWALCheckpoint verifies that checkpoint operations handle arbitrary
// offsets safely.
func FuzzWALCheckpoint(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(100))

	f.Fuzz(func(t *testing.T, offset uint64) {
		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}
		defer w.Close()

		// Append some data
		_, _ = w.Append(EntryMessage, []byte("data"))

		// Checkpoint should not panic
		_ = w.Checkpoint(offset)
	})
}

// FuzzWALMultipleAppends verifies that multiple sequential appends work
// safely with arbitrary data.
func FuzzWALMultipleAppends(f *testing.F) {
	f.Add([]byte("msg1"), []byte("msg2"), []byte("msg3"))

	f.Fuzz(func(t *testing.T, d1, d2, d3 []byte) {
		// Cap each to 64KB
		if len(d1) > 65536 {
			d1 = d1[:65536]
		}
		if len(d2) > 65536 {
			d2 = d2[:65536]
		}
		if len(d3) > 65536 {
			d3 = d3[:65536]
		}

		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}
		defer w.Close()

		for _, data := range [][]byte{d1, d2, d3} {
			_, _ = w.Append(EntryMessage, data)
		}
	})
}

// FuzzWALSegmentPath verifies that segment path generation handles
// arbitrary sequences safely.
func FuzzWALSegmentPath(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(999999))

	f.Fuzz(func(t *testing.T, seq uint64) {
		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}
		defer w.Close()

		// segmentPath should not panic
		path := w.segmentPath(seq)
		if path == "" {
			t.Error("expected non-empty segment path")
		}
	})
}

// FuzzWALListSegments verifies that segment listing handles arbitrary
// directory contents safely.
func FuzzWALListSegments(f *testing.F) {
	f.Add("00000000000000000000.wal")
	f.Add("not-a-segment.wal")
	f.Add("")

	f.Fuzz(func(t *testing.T, filename string) {
		dir, err := os.MkdirTemp("", "wal-fuzz-*")
		if err != nil {
			t.Skip("cannot create temp dir")
		}
		defer os.RemoveAll(dir)

		// Create a file with the fuzz name
		if filename != "" {
			f, err := os.Create(filepath.Join(dir, filename))
			if err == nil {
				f.Close()
			}
		}

		w, err := NewWAL(dir, 1024*1024, SyncImmediate, time.Second)
		if err != nil {
			t.Skipf("cannot create WAL: %v", err)
		}
		defer w.Close()

		// listSegments should not panic
		_, _ = w.listSegments()
	})
}
