package wal

import (
	"os"
	"testing"
)

func BenchmarkWALAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 64*1024*1024, SyncOS, 0)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(EntryMessage, data); err != nil {
			b.Fatal(err)
		}
	}
	w.Close()
}

func BenchmarkWALAppendBatch(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-walbatch-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := NewWAL(dir, 64*1024*1024, SyncOS, 0)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()

	// Batch of 100
	batch := 100
	i := 0
	for i < b.N {
		end := i + batch
		if end > b.N {
			end = b.N
		}
		for j := i; j < end; j++ {
			w.Append(EntryMessage, data)
		}
		i = end
	}
	w.Close()
}
