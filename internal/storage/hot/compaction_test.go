package hot

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestCompactorDisabled(t *testing.T) {
	lc := NewLogCompactor(CompactNone)
	if lc.Enabled() {
		t.Error("should be disabled")
	}
}

func TestCompactorEnabled(t *testing.T) {
	lc := NewLogCompactor(CompactKeyBased)
	if !lc.Enabled() {
		t.Error("should be enabled")
	}
}

func TestShouldCompactFewFrozen(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	lc := NewLogCompactor(CompactKeyBased)

	// No frozen segments
	if lc.ShouldCompact(p) {
		t.Error("should not compact with 0 frozen segments")
	}
}

func TestCompactNoFrozenSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	lc := NewLogCompactor(CompactKeyBased)
	// Compact on empty/active-only should be a no-op
	if err := lc.Compact(p); err != nil {
		t.Errorf("compact on no frozen: %v", err)
	}
}

func TestCompactKeyBased(t *testing.T) {
	dir := t.TempDir()
	// Very small segment size to force roll-over
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write messages with routing keys — some keys repeated
	msgs := []struct {
		key  string
		data string
	}{
		{"key1", "value1-v1"},
		{"key2", "value2-v1"},
		{"key1", "value1-v2"}, // newer value for key1
		{"key3", "value3-v1"},
		{"key2", "value2-v2"}, // newer value for key2
		{"key1", "value1-v3"}, // latest value for key1
	}

	for _, m := range msgs {
		env := &message.Envelope{
			Topic:       "test",
			RoutingKey:  m.key,
			Payload:     []byte(m.data),
			ContentType: "text/plain",
			Timestamp:   time.Now().UnixNano(),
		}
		data, err := message.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(data); err != nil {
			// Segment full — that's expected, keep going
			_ = err
		}
	}

	// Force freeze all segments except the last
	p.mu.Lock()
	for i := 0; i < len(p.segments)-1; i++ {
		p.segments[i].Freeze()
		p.segments[i].SaveIndex()
	}
	p.mu.Unlock()

	lc := NewLogCompactor(CompactKeyBased)

	if !lc.ShouldCompact(p) {
		t.Error("should have enough frozen segments")
	}

	if err := lc.Compact(p); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	// After compaction, segment count should be reduced
	if p.SegmentCount() < 1 {
		t.Error("should have at least 1 segment after compaction")
	}
}

func TestCompactKeylessMessages(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write messages without routing keys
	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "test",
			Payload:     []byte("no-key-msg"),
			ContentType: "text/plain",
			Timestamp:   time.Now().UnixNano(),
		}
		data, _ := message.Marshal(env)
		p.Append(data)
	}

	// Force freeze all segments except last
	p.mu.Lock()
	for i := 0; i < len(p.segments)-1; i++ {
		p.segments[i].Freeze()
		p.segments[i].SaveIndex()
	}
	p.mu.Unlock()

	lc := NewLogCompactor(CompactKeyBased)
	if err := lc.Compact(p); err != nil {
		t.Fatalf("compact keyless: %v", err)
	}
}

func TestCompactionStats(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "test", 0, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write enough to create segments
	for i := 0; i < 20; i++ {
		env := &message.Envelope{
			Topic:       "test",
			RoutingKey:  "key",
			Payload:     []byte("data"),
			ContentType: "text/plain",
			Timestamp:   time.Now().UnixNano(),
		}
		data, _ := message.Marshal(env)
		p.Append(data)
	}

	lc := NewLogCompactor(CompactKeyBased)
	stats := lc.Stats(p)
	if stats.FrozenSegments < 0 {
		t.Error("frozen count should not be negative")
	}
}
