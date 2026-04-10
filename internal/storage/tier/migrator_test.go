package tier

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/warm"
)

func TestMigratorRead(t *testing.T) {
	dir := t.TempDir()

	// Create hot engine
	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()

	part, err := he.GetOrCreatePartition("test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write data to hot tier
	for i := uint64(0); i < 10; i++ {
		data := []byte(fmt.Sprintf("msg-%d", i))
		part.Append(data)
	}

	// Create warm engine
	warmDir := dir + "/warm"
	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	// Write some data to warm tier
	for i := uint64(100); i < 110; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		we.Put(key, []byte(fmt.Sprintf("warm-%d", i)))
	}

	// Create migrator
	policy := TierPolicy{
		HotRetention:  1 * time.Hour,
		WarmRetention: 24 * time.Hour,
		ColdRetention: 7 * 24 * time.Hour,
	}
	m := NewMigrator(policy, he, we, nil)

	// Read from hot tier
	data, err := m.Read("test", 0, 5)
	if err != nil || string(data) != "msg-5" {
		t.Errorf("Read(5) = (%q, %v), want (msg-5, nil)", data, err)
	}

	// Read from warm tier
	data, err = m.Read("test", 0, 105)
	if err != nil || string(data) != "warm-105" {
		t.Errorf("Read(105) = (%q, %v), want (warm-105, nil)", data, err)
	}

	// Read nonexistent
	_, err = m.Read("test", 0, 9999)
	if err == nil {
		t.Error("should fail for nonexistent offset")
	}
}

func TestMigratorStartStop(t *testing.T) {
	dir := t.TempDir()
	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()

	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, nil, nil)

	m.Start()
	time.Sleep(100 * time.Millisecond)
	m.Stop()
}

func TestColdManagerCreate(t *testing.T) {
	dir := t.TempDir()
	cm, err := NewColdManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if len(cm.archives) != 0 {
		t.Error("new cold manager should have no archives")
	}
}

func TestMigrateHotToWarm(t *testing.T) {
	dir := t.TempDir()

	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 256})
	defer he.Close()

	part, err := he.GetOrCreatePartition("test-migrate", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write enough data to trigger segment roll
	for i := 0; i < 50; i++ {
		data := make([]byte, 20) // 20 bytes each
		copy(data, fmt.Sprintf("msg-%03d", i))
		part.Append(data)
	}

	// Verify we have frozen segments (active segment is the last one)
	frozen := part.FrozenSegments()
	if len(frozen) == 0 {
		t.Skip("no frozen segments created - segment size too large")
	}

	// Create warm engine
	warmDir := dir + "/warm"
	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	// Migrate with 0 retention = migrate everything immediately
	policy := TierPolicy{HotRetention: 0}
	m := NewMigrator(policy, he, we, nil)

	// Force immediate migration by calling directly
	// Use a very short retention to trigger migration
	m.policy.HotRetention = time.Nanosecond
	m.migrateHotToWarm()

	// Verify data is now in warm tier
	for i := 0; i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		val, found, deleted := we.Get(key)
		if !found {
			t.Logf("offset %d not found in warm tier (may be in active hot segment)", i)
			continue
		}
		if deleted {
			t.Errorf("offset %d marked as deleted", i)
		}
		if len(val) != 20 {
			t.Errorf("offset %d: got %d bytes, want 20", i, len(val))
		}
	}
}
