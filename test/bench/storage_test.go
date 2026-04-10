package bench

import (
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/wal"
)

// BenchmarkWALRecovery benchmarks replaying 10K entries from WAL.
func BenchmarkWALRecovery(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-recover-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Pre-populate WAL with entries
	w, err := wal.NewWAL(dir, 64*1024*1024, wal.SyncOS, 0)
	if err != nil {
		b.Fatal(err)
	}
	data := make([]byte, 256)
	const msgCount = 10000
	for i := 0; i < msgCount; i++ {
		w.Append(wal.EntryMessage, data)
	}
	w.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w2, _ := wal.NewWAL(dir, 64*1024*1024, wal.SyncOS, 0)
		count := 0
		w2.Recover(0, func(_ wal.EntryType, _ []byte) error {
			count++
			return nil
		})
		w2.Close()
		if count != msgCount {
			b.Fatalf("expected %d entries, got %d", msgCount, count)
		}
	}
}

// BenchmarkWALAppendSyncImmediate benchmarks WAL append with immediate sync.
func BenchmarkWALAppendSyncImmediate(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-sync-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := wal.NewWAL(dir, 64*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 128)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.Append(wal.EntryMessage, data)
	}
	w.Close()
}

// BenchmarkWALCheckpoint benchmarks checkpoint write + read cycle.
func BenchmarkWALCheckpoint(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-wal-cp-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := wal.NewWAL(dir, 64*1024*1024, wal.SyncOS, 0)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.Checkpoint(uint64(i))
	}
	w.Close()
}
