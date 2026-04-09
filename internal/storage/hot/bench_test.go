package hot

import (
	"fmt"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func BenchmarkSegmentAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-seg-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := OpenSegment(dir+"/bench.log", 0, 256*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer seg.Close()

	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := seg.Append(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-read-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	seg, err := OpenSegment(dir+"/bench.log", 0, 256*1024*1024)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 256)
	msgCount := 10000
	for i := 0; i < msgCount; i++ {
		seg.Append(data)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		seg.ReadAt(int64(32 + (i%msgCount)*(4+256))) // header + record
	}
	seg.Close()
}

func BenchmarkPartitionAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-part-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := NewEngine(dir, HotConfig{SegmentSize: 64 * 1024 * 1024})
	part, err := engine.GetOrCreatePartition("bench-topic", 0)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := part.Append(data); err != nil {
			b.Fatal(err)
		}
	}
	engine.Close()
}

func BenchmarkPartitionRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-partread-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := NewEngine(dir, HotConfig{SegmentSize: 64 * 1024 * 1024})
	part, err := engine.GetOrCreatePartition("bench-read", 0)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 256)
	for i := 0; i < 10000; i++ {
		part.Append(data)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		part.Read(uint64(i % 10000))
	}
	engine.Close()
}

func BenchmarkSparseIndex(b *testing.B) {
	si := &SparseIndex{
		entries:  make([]IndexEntry, 0, 100000),
		interval: 256,
	}
	for i := uint64(0); i < 100000; i++ {
		si.Add(i, uint32(i*300), int64(i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % 100000)
		si.Search(offset)
	}
}

func BenchmarkEngineGetOrCreatePartition(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-engine-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := NewEngine(dir, HotConfig{SegmentSize: 64 * 1024 * 1024})

	// Pre-create partitions
	for i := 0; i < 16; i++ {
		engine.GetOrCreatePartition(fmt.Sprintf("topic-%d", i), 0)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("topic-%d", i%16)
		engine.GetOrCreatePartition(topic, 0)
	}
	engine.Close()
}

// BenchmarkMessageRoundtrip benchmarks full message marshal → storage → read → unmarshal
func BenchmarkMessageRoundtrip(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-roundtrip-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	engine := NewEngine(dir, HotConfig{SegmentSize: 64 * 1024 * 1024})
	part, _ := engine.GetOrCreatePartition("roundtrip", 0)

	env := &message.Envelope{
		Topic:       "roundtrip",
		Payload:     make([]byte, 256),
		ContentType: "application/octet-stream",
		SourceProto: message.ProtoHTTP,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := message.Marshal(env)
		offset, _ := part.Append(data)
		readData, _ := part.Read(offset)
		message.Unmarshal(readData)
	}
	engine.Close()
}
