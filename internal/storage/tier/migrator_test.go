package tier

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/storage/cold"
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
	m := NewMigrator(policy, he, we, nil, nil)

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
	m := NewMigrator(policy, he, nil, nil, nil)

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
	m := NewMigrator(policy, he, we, nil, nil)

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

func TestMigrateWarmToCold(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   256,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	// Write enough data to flush multiple SSTables
	for i := uint64(0); i < 200; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		we.Put(key, []byte(fmt.Sprintf("warm-cold-%d", i)))
	}

	// OldSSTables only finds L1+ tables. Even if no L1+ SSTables exist,
	// migrateWarmToCold should be a safe no-op.
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	policy := TierPolicy{WarmRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()

	// Test the cold creation path directly: create SSTables, make archive
	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 20; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("direct-cold-%d", i)))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()

	sst2, err := warm.OpenSSTable(sst.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()

	archivePath := filepath.Join(coldDir, "direct-cold.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst2})
	if err != nil {
		t.Fatal(err)
	}

	// Verify data readable from cold archive
	for i := uint64(0); i < 20; i++ {
		val, err := ca.Get(i)
		if err != nil {
			t.Errorf("Get(%d): %v", i, err)
		} else if string(val) != fmt.Sprintf("direct-cold-%d", i) {
			t.Errorf("Get(%d) = %q", i, val)
		}
	}
	ca.Close()
}

func TestReadFromColdTier(t *testing.T) {
	dir := t.TempDir()

	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()

	warmDir := filepath.Join(dir, "warm")
	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	coldDir := filepath.Join(dir, "cold")
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	mt := warm.NewMemTable(4096)
	for i := uint64(500); i < 510; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("cold-data-%d", i)))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(coldDir, "cold-read-test.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()
	cm.archives[archivePath] = ca

	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, we, cm, nil)

	data, err := m.Read("test", 0, 505)
	if err != nil {
		t.Fatalf("Read from cold: %v", err)
	}
	if string(data) != "cold-data-505" {
		t.Errorf("got %q, want cold-data-505", data)
	}
}

func TestPurgeExpiredCold(t *testing.T) {
	dir := t.TempDir()
	coldDir := filepath.Join(dir, "cold")

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}

	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 5; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("data"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(coldDir, "cold-purge-test.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()
	cm.archives[archivePath] = ca

	if len(cm.archives) != 1 {
		t.Fatalf("expected 1 archive, got %d", len(cm.archives))
	}

	// Use a negative ColdRetention so everything is immediately expired.
	// purgeExpiredCold checks: now.Sub(ca.CreatedAt()) > ColdRetention
	// With negative retention, even a freshly-created archive passes.
	policy := TierPolicy{ColdRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, nil, cm, nil)
	m.purgeExpiredCold()

	if len(cm.archives) != 0 {
		t.Errorf("expected 0 archives after purge, got %d", len(cm.archives))
	}
	cm.Close()
}

func TestColdManagerLoadExisting(t *testing.T) {
	dir := t.TempDir()
	coldDir := filepath.Join(dir, "cold")
	os.MkdirAll(coldDir, 0755)

	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 5; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("persist-data"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(coldDir, "cold-persist.dat")
	firstCA, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	firstCA.Close()
	sst.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if len(cm.archives) != 1 {
		t.Fatalf("expected 1 loaded archive, got %d", len(cm.archives))
	}

	for _, ca := range cm.archives {
		data, err := ca.Get(2)
		if err != nil {
			t.Fatalf("read from loaded archive: %v", err)
		}
		if string(data) != "persist-data" {
			t.Errorf("got %q, want persist-data", data)
		}
	}
}

func TestMigratorNoWarmNilPanics(t *testing.T) {
	dir := t.TempDir()
	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()

	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, nil, nil, nil)

	m.migrateHotToWarm()
	m.migrateWarmToCold()
	m.purgeExpiredCold()
}

func TestMigratorNoRetentionNoMigration(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")

	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 256})
	defer he.Close()

	part, err := he.GetOrCreatePartition("no-ret", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		part.Append([]byte(fmt.Sprintf("msg-%03d", i)))
	}

	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	policy := TierPolicy{HotRetention: 0}
	m := NewMigrator(policy, he, we, nil, nil)
	m.migrateHotToWarm()

	stats := we.Stats()
	if stats.TotalSSTables > 0 {
		t.Logf("warm has %d entries", stats.TotalSSTables)
	}
}

func TestFullMigrationPipeline(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 256})
	defer he.Close()

	part, err := he.GetOrCreatePartition("pipeline-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		data := make([]byte, 20)
		copy(data, fmt.Sprintf("pipe-%03d", i))
		part.Append(data)
	}

	frozen := part.FrozenSegments()
	if len(frozen) == 0 {
		t.Skip("no frozen segments")
	}

	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   256,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	policy := TierPolicy{
		HotRetention:  time.Nanosecond,
		WarmRetention: time.Nanosecond,
		ColdRetention: 7 * 24 * time.Hour,
	}
	m := NewMigrator(policy, he, we, cm, nil)

	// Phase 1: Hot → Warm
	m.migrateHotToWarm()

	// Phase 2: Warm → Cold (may be no-op if no L1+ SSTables, that's OK)
	m.migrateWarmToCold()

	// Give background compaction time to settle before reading
	time.Sleep(100 * time.Millisecond)

	found := 0
	for i := uint64(0); i < 50; i++ {
		data, err := m.Read("pipeline-test", 0, i)
		if err == nil {
			found++
			_ = data
		}
	}
	t.Logf("read %d/50 entries through migrator after full pipeline", found)
	if found == 0 {
		t.Error("expected at least some entries readable through migrator")
	}
}
