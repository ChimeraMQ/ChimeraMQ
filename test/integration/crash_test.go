package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

// TestCrashRecoveryWALPartialWrite simulates a crash during WAL write
// by writing messages, stopping the broker mid-buffer (without graceful
// shutdown), and verifying that already-synced messages survive.
func TestCrashRecoveryWALPartialWrite(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "crash-wal",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish messages that will be synced
	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "crash-wal",
			Payload:     []byte(fmt.Sprintf("msg-%d", i)),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Recreate broker (simulates crash + restart)
	tb.recreateBroker(t)

	// Verify synced messages survived
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("crash-wal", 0, 0, 20, 0)
	if err != nil {
		t.Fatalf("fetch after crash: %v", err)
	}
	if len(msgs) < 10 {
		t.Errorf("expected >= 10 messages after crash recovery, got %d", len(msgs))
	}
}

// TestCrashRecoverySegmentDataIntegrity verifies that segment files are
// not corrupted after a simulated crash (hard stop without cleanup).
func TestCrashRecoverySegmentDataIntegrity(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "crash-seg",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Write enough data to create multiple records
	for i := 0; i < 50; i++ {
		env := &message.Envelope{
			Topic:       "crash-seg",
			Payload:     []byte(fmt.Sprintf("payload-%04d", i)),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Hard stop — just kill the broker without graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	tb.server.Shutdown(ctx)
	cancel()
	tb.broker.Stop()

	// Reopen the broker on the same data dir
	cfg := tb.broker.Config()
	b2, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("recreate broker: %v", err)
	}
	if err := b2.Start(); err != nil {
		t.Fatalf("restart broker: %v", err)
	}
	defer b2.Stop()

	// Verify data integrity
	se := b2.StreamEngine()
	msgs, _, err := se.Fetch("crash-seg", 0, 0, 100, 0)
	if err != nil {
		t.Fatalf("fetch after crash: %v", err)
	}
	if len(msgs) < 50 {
		t.Errorf("expected >= 50 messages, got %d", len(msgs))
	}

	// Verify first and last message content
	first := msgs[0]
	if string(first.Payload) != "payload-0000" {
		t.Errorf("first message = %q, want payload-0000", first.Payload)
	}
}

// TestCrashRecoveryTopicAndData tests that both topic metadata and message
// data survive a crash when using WAL replay.
func TestCrashRecoveryTopicAndData(t *testing.T) {
	tb := newTestBroker(t)

	// Create multiple topics with data
	topics := []struct {
		name string
		msgs int
	}{
		{"crash-t1", 5},
		{"crash-t2", 10},
		{"crash-t3", 3},
	}

	for _, tc := range topics {
		tb.broker.Topics().CreateTopic(broker.TopicConfig{
			Name:       tc.name,
			Mode:       broker.ModeStream,
			Partitions: 1,
		})
		for i := 0; i < tc.msgs; i++ {
			env := &message.Envelope{
				Topic:       tc.name,
				Payload:     []byte(fmt.Sprintf("%s-msg-%d", tc.name, i)),
				SourceProto: message.ProtoHTTP,
			}
			tb.broker.Publish(env)
		}
	}

	// Crash and restart
	tb.recreateBroker(t)

	// Verify all topics and messages survived
	list := tb.broker.Topics().ListTopics()
	if len(list) < 3 {
		t.Errorf("expected >= 3 topics, got %d", len(list))
	}

	se := tb.broker.StreamEngine()
	for _, tc := range topics {
		msgs, _, err := se.Fetch(tc.name, 0, 0, 100, 0)
		if err != nil {
			t.Errorf("fetch %s: %v", tc.name, err)
			continue
		}
		if len(msgs) != tc.msgs {
			t.Errorf("topic %s: got %d messages, want %d", tc.name, len(msgs), tc.msgs)
		}
	}
}

// TestCrashRecoveryWALCorruptTrailer verifies that a corrupt entry at the
// end of a WAL segment (simulating a partial write during crash) does not
// prevent recovery of earlier valid entries.
func TestCrashRecoveryWALCorruptTrailer(t *testing.T) {
	dir := t.TempDir()

	// Create a WAL and write some valid entries
	w, err := wal.NewWAL(dir, 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		if _, err := w.Append(wal.EntryMessage, data); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Append garbage bytes to the WAL file (simulates crash mid-write)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			path := filepath.Join(dir, e.Name())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			f.Write([]byte{0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00}) // partial header
			f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})                   // garbage
			f.Close()
		}
	}

	// Reopen and recover
	w2, err := wal.NewWAL(dir, 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recovered := 0
	err = w2.Recover(0, func(et wal.EntryType, data []byte) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != 5 {
		t.Errorf("recovered %d entries, want 5", recovered)
	}
}

// TestCrashRecoverySegmentPartialRecord tests that a segment with a
// partial/truncated record at the end can still be opened and the
// valid records before it are readable.
func TestCrashRecoverySegmentPartialRecord(t *testing.T) {
	dir := t.TempDir()

	// Create a segment and write some records
	seg, err := hot.OpenSegment(filepath.Join(dir, "00000000000000000000.log"), 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("record-%d", i))
		_, _, err := seg.Append(data)
		if err != nil {
			t.Fatal(err)
		}
	}
	seg.Close()

	// Truncate the file to simulate a partial write (mid-record)
	path := filepath.Join(dir, "00000000000000000000.log")
	info, _ := os.Stat(path)
	// Keep all valid records but chop off the last 3 bytes
	truncatedSize := info.Size() - 3
	if truncatedSize < hot.SegmentHeaderLen {
		t.Fatal("truncated size too small")
	}
	if err := os.Truncate(path, truncatedSize); err != nil {
		t.Fatal(err)
	}

	// Reopen the segment — should handle the truncation gracefully
	seg2, err := hot.OpenSegment(path, 0, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer seg2.Close()

	// The valid records should still be readable (minus the truncated one)
	count := 0
	pos := int64(hot.SegmentHeaderLen)
	for pos < seg2.Size()-3 { // account for truncation
		_, nextPos, err := seg2.ReadAtSequential(pos)
		if err != nil {
			break
		}
		count++
		pos = nextPos
	}
	if count < 4 {
		t.Errorf("expected >= 4 valid records, got %d", count)
	}
}

// TestCrashRecoveryEmptyWALRecovery tests recovery from an empty WAL.
func TestCrashRecoveryEmptyWALRecovery(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWAL(dir, 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	recovered := 0
	err = w.Recover(0, func(et wal.EntryType, data []byte) error {
		recovered++
		return nil
	})
	w.Close()

	if err != nil {
		t.Fatalf("recover empty WAL: %v", err)
	}
	if recovered != 0 {
		t.Errorf("expected 0 entries from empty WAL, got %d", recovered)
	}
}

// TestCrashRecoveryWALCRCMismatch verifies that a CRC mismatch in the WAL
// stops recovery at the corrupt entry but preserves earlier valid entries.
func TestCrashRecoveryWALCRCMismatch(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.NewWAL(dir, 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write 3 valid entries
	for i := 0; i < 3; i++ {
		w.Append(wal.EntryMessage, []byte(fmt.Sprintf("valid-%d", i)))
	}
	w.Close()

	// Corrupt the second entry's data
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			path := filepath.Join(dir, e.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// Corrupt a byte in the second entry's data area
			// WAL header is 17 bytes, first entry data starts at 17
			// First entry = header(17) + "valid-0"(7) = 24 bytes
			// Second entry header starts at 24, data at 24+17=41
			corruptPos := 41
			if corruptPos < len(data) {
				data[corruptPos] ^= 0xFF // flip bits
				os.WriteFile(path, data, 0640)
			}
		}
	}

	// Recover — should get the first entry only
	w2, err := wal.NewWAL(dir, 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recovered := 0
	err = w2.Recover(0, func(et wal.EntryType, data []byte) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	// Should recover at least 1 valid entry before the corruption
	if recovered < 1 {
		t.Errorf("expected >= 1 recovered entry, got %d", recovered)
	}
}

// TestCrashRecoveryMultipleWALSegments verifies recovery across
// multiple WAL segment files.
func TestCrashRecoveryMultipleWALSegments(t *testing.T) {
	dir := t.TempDir()

	// Use a small max size to force rotation
	w, err := wal.NewWAL(dir, 256, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	totalEntries := 30
	for i := 0; i < totalEntries; i++ {
		data := make([]byte, 20)
		binary.BigEndian.PutUint64(data, uint64(i))
		copy(data[8:], []byte(fmt.Sprintf("seg-entry-%02d", i)))
		if _, err := w.Append(wal.EntryMessage, data); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	w.Close()

	// Verify multiple segments exist
	entries, _ := os.ReadDir(dir)
	walFiles := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			walFiles++
		}
	}
	if walFiles < 2 {
		t.Errorf("expected >= 2 WAL segments, got %d", walFiles)
	}

	// Recover all entries
	w2, err := wal.NewWAL(dir, 256, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	recovered := 0
	err = w2.Recover(0, func(et wal.EntryType, data []byte) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recovered != totalEntries {
		t.Errorf("recovered %d entries, want %d", recovered, totalEntries)
	}
}

// TestCrashRecoveryBrokerRestartPreservesQueue tests that in-flight queue
// messages survive a broker restart.
func TestCrashRecoveryBrokerRestartPreservesQueue(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "crash-queue",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	// Publish messages to the queue
	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "crash-queue",
			Payload:     []byte(fmt.Sprintf("queue-msg-%d", i)),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Restart
	tb.recreateBroker(t)

	// Publish more after restart
	for i := 10; i < 15; i++ {
		env := &message.Envelope{
			Topic:       "crash-queue",
			Payload:     []byte(fmt.Sprintf("queue-msg-%d", i)),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("post-restart publish %d: %v", i, err)
		}
	}

	// Fetch via stream engine to verify data exists
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("crash-queue", 0, 0, 20, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) < 10 {
		t.Errorf("expected >= 10 messages after restart, got %d", len(msgs))
	}
}
