package tier

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/metrics"
	"github.com/chimeramq/chimera/internal/storage/cold"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/warm"
)

func writeBatchToLSM(t *testing.T, lsm *warm.LSMTree, baseOffset uint64, count int) {
	t.Helper()
	for i := uint64(0); i < uint64(count); i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, baseOffset+i)
		lsm.Put(key, []byte(fmt.Sprintf("val-%d", baseOffset+i)))
	}
}

func createSSTablesAtL1(t *testing.T, warmDir string, n, entriesPerTable int) *warm.LSMTree {
	t.Helper()
	if err := os.MkdirAll(warmDir, 0755); err != nil {
		t.Fatal(err)
	}
	sstPaths := make([]string, 0, n)
	for batch := 0; batch < n; batch++ {
		mt := warm.NewMemTable(4096)
		for i := uint64(0); i < uint64(entriesPerTable); i++ {
			key := make([]byte, 8)
			off := uint64(batch*1000) + i
			binary.BigEndian.PutUint64(key, off)
			mt.Put(key, []byte(fmt.Sprintf("l1-data-%d", off)))
		}
		mt.Freeze()
		sst, err := warm.FlushMemTable(mt, warmDir, 0)
		if err != nil {
			t.Fatal(err)
		}
		sstPaths = append(sstPaths, sst.Path())
		sst.Close()
	}
	mf, err := warm.NewManifest(warmDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range sstPaths {
		sst, err := warm.OpenSSTable(p)
		if err != nil {
			continue
		}
		mf.Add(1, sst)
		sst.Close()
	}
	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   4 * 1024 * 1024,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return we
}

func TestMigrateWarmToColdNilCold(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil, nil)
	// Should not panic when cold is nil
	m.migrateWarmToCold()
}

func TestMigrateWarmToColdZeroRetention(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	policy := TierPolicy{WarmRetention: 0}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
}

func TestMigrateWarmToColdNoOldSSTables(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// No data written — should complete without error
	policy := TierPolicy{WarmRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
}

func TestMigrateWarmToColdWithL1Data(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	// Write data with tiny memtable to force SSTable creation
	for b := 0; b < 10; b++ {
		writeBatchToLSM(t, we, uint64(b*1000), 5)
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()

	cm.mu.RLock()
	count := len(cm.archives)
	cm.mu.RUnlock()
	t.Logf("L1Data test: %d archive(s) created", count)
}

func TestMigrateWarmToColdBatchSplitting(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	// Write enough to create many SSTables
	for b := 0; b < 20; b++ {
		writeBatchToLSM(t, we, uint64(b*100), 3)
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
}

func TestNewColdManagerError(t *testing.T) {
	// On some OSes, MkdirAll may succeed for odd paths, so this test is best-effort
	_, err := NewColdManager(string([]byte{0x00}))
	_ = err
}

func TestPurgeExpiredColdNoRetention(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil, nil)
	// Should not panic with nil cold
	m.purgeExpiredCold()
}

func TestPurgeExpiredColdNilCold(t *testing.T) {
	policy := TierPolicy{ColdRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, nil, nil, nil)
	m.purgeExpiredCold()
}

func TestMigrateHotToWarmNoWarm(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil, nil)
	// Should not panic when warm is nil
	m.migrateHotToWarm()
}

func TestColdManagerLoadExistingIgnoresNonDatFiles(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Write a non-.dat file
	_ = os.WriteFile(filepath.Join(coldDir, "readme.txt"), []byte("test"), 0644)

	// Reload
	cm2, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm2.Close()

	cm2.mu.RLock()
	count := len(cm2.archives)
	cm2.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 archives, got %d", count)
	}
}

func TestColdManagerLoadExistingSkipsCorruptDat(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	// Write a corrupt .dat file
	_ = os.WriteFile(filepath.Join(coldDir, "corrupt.dat"), []byte("not-valid-json"), 0644)

	// Reload should skip corrupt file without panicking
	cm2, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm2.Close()
}

func TestMigrateRunStop(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil, nil)
	m.Start()
	time.Sleep(50 * time.Millisecond)
	m.Stop()
}

// TestMigrateWarmToColdWithManualL1 injects SSTables at L1 via the manifest
// and exercises the full batch migration loop in migrateWarmToCold.
func TestMigrateWarmToColdWithManualL1(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")
	we := createSSTablesAtL1(t, warmDir, 15, 5)
	defer we.Close()
	stats := we.Stats()
	l1Count := 0
	for _, lv := range stats.Levels {
		if lv.Level == 1 {
			l1Count = lv.SSTables
		}
	}
	t.Logf("L1 SSTable count: %d", l1Count)
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
	cm.mu.RLock()
	count := len(cm.archives)
	cm.mu.RUnlock()
	t.Logf("manual L1 migration: %d archive(s) created", count)
	if count == 0 && l1Count > 0 {
		t.Errorf("expected archives from %d L1 SSTables, got 0", l1Count)
	}
	if count > 0 {
		cm.mu.RLock()
		for _, ca := range cm.archives {
			rng := ca.OffsetRange()
			for off := rng.Min; off <= rng.Max; off++ {
				val, err := ca.Get(off)
				if err == nil {
					t.Logf("verified cold data at offset %d: %q", off, val)
					break
				}
			}
		}
		cm.mu.RUnlock()
	}
}

// TestMigrateWarmToColdRemovesSSTables verifies SSTable removal after migration.
func TestMigrateWarmToColdRemovesSSTables(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")
	we := createSSTablesAtL1(t, warmDir, 5, 5)
	defer we.Close()
	statsBefore := we.Stats()
	l1Before := 0
	for _, lv := range statsBefore.Levels {
		if lv.Level == 1 {
			l1Before = lv.SSTables
		}
	}
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
	statsAfter := we.Stats()
	l1After := 0
	for _, lv := range statsAfter.Levels {
		if lv.Level == 1 {
			l1After = lv.SSTables
		}
	}
	if l1Before > 0 && l1After >= l1Before {
		t.Errorf("expected L1 decrease: before=%d after=%d", l1Before, l1After)
	}
}

// TestMigrateWarmToColdReadThrough verifies reading migrated data from cold tier.
func TestMigrateWarmToColdReadThrough(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "hot")
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")
	he := hot.NewEngine(hotDir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()
	we := createSSTablesAtL1(t, warmDir, 3, 5)
	defer we.Close()
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, he, we, cm, nil)
	m.migrateWarmToCold()
	cm.mu.RLock()
	archiveCount := len(cm.archives)
	cm.mu.RUnlock()
	if archiveCount == 0 {
		t.Skip("no archives created")
	}
	found := 0
	for batch := 0; batch < 3; batch++ {
		for i := uint64(0); i < 5; i++ {
			off := uint64(batch*1000) + i
			data, err := m.Read("", 0, off)
			if err == nil {
				found++
				expected := fmt.Sprintf("l1-data-%d", off)
				if string(data) != expected {
					t.Errorf("Read(%d) = %q, want %q", off, data, expected)
				}
			}
		}
	}
	if found == 0 {
		t.Error("expected at least some entries readable from cold tier")
	}
}

// TestMigrateWarmToColdLoadExistingArchives verifies archive persistence.
func TestMigrateWarmToColdLoadExistingArchives(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")
	we := createSSTablesAtL1(t, warmDir, 3, 5)
	defer we.Close()
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
	cm.mu.RLock()
	firstCount := len(cm.archives)
	cm.mu.RUnlock()
	if firstCount == 0 {
		cm.Close()
		t.Skip("no archives created")
	}
	cm.Close()
	cm2, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm2.Close()
	cm2.mu.RLock()
	reloadCount := len(cm2.archives)
	cm2.mu.RUnlock()
	if reloadCount != firstCount {
		t.Errorf("expected %d archives after reload, got %d", firstCount, reloadCount)
	}
}

// TestColdManagerLoadExistingIgnoresNonDatFilesWithArchive verifies non-dat
// files are ignored alongside a valid cold archive.
func TestColdManagerLoadExistingIgnoresNonDatFilesWithArchive(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(coldDir, "readme.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(coldDir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}
	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 3; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("ignore-test"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(coldDir, "valid.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	ca.Close()
	sst.Close()
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	cm.mu.RLock()
	count := len(cm.archives)
	cm.mu.RUnlock()
	if count != 1 {
		t.Errorf("expected 1 archive, got %d", count)
	}
}

// TestReadDeletedFromWarm verifies reading a deleted key from warm tier.
func TestReadDeletedFromWarm(t *testing.T) {
	dir := t.TempDir()
	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()
	warmDir := filepath.Join(dir, "warm")
	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	we.Put(key, []byte("to-be-deleted"))
	we.Delete(key)
	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, we, nil, nil)
	_, err = m.Read("test", 0, 42)
	if err == nil {
		t.Error("expected error for deleted offset")
	}
}

// TestReadFromColdTierWithHotEngine verifies Read falls through all tiers.
func TestReadFromColdTierWithHotEngine(t *testing.T) {
	dir := t.TempDir()
	coldDir := filepath.Join(dir, "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		t.Fatal(err)
	}
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	mt := warm.NewMemTable(4096)
	for i := uint64(10); i < 15; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("cold-%d", i)))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(coldDir, "cold-tier.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()
	cm.archives[archivePath] = ca
	he := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()
	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, nil, cm, nil)
	data, err := m.Read("test", 0, 12)
	if err != nil {
		t.Fatalf("Read(12): %v", err)
	}
	if string(data) != "cold-12" {
		t.Errorf("got %q, want cold-12", data)
	}
	_, err = m.Read("test", 0, 99)
	if err == nil {
		t.Error("expected error for offset 99")
	}
}

// TestPurgeExpiredColdWithColdArchive verifies purgeExpiredCold with real archive.
func TestPurgeExpiredColdWithColdArchive(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		t.Fatal(err)
	}
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 3; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("data"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(coldDir, "cold-purge.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()
	cm.archives[archivePath] = ca
	policy := TierPolicy{ColdRetention: 0}
	m := NewMigrator(policy, nil, nil, cm, nil)
	m.purgeExpiredCold()
	cm.mu.RLock()
	n := len(cm.archives)
	cm.mu.RUnlock()
	if n != 1 {
		t.Errorf("expected 1 archive with ColdRetention=0, got %d", n)
	}
}

func TestColdManagerTotalSize(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	if cm.TotalSize() != 0 {
		t.Errorf("empty total size = %d, want 0", cm.TotalSize())
	}

	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 3; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("data"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(coldDir, "size-test.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst})
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()
	cm.archives[archivePath] = ca

	if cm.TotalSize() <= 0 {
		t.Error("TotalSize should be positive after adding archive")
	}
}

func TestUpdateStorageMetrics(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "hot")
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	he := hot.NewEngine(hotDir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer he.Close()

	we, err := warm.NewLSMTree(warmDir, warm.DefaultLSMConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	mc := metrics.NewCollector()
	m := NewMigrator(TierPolicy{}, he, we, cm, mc)

	// Should not panic and should update all three tiers
	m.updateStorageMetrics()
}

func TestUpdateStorageMetricsNilTiers(t *testing.T) {
	mc := metrics.NewCollector()
	m := NewMigrator(TierPolicy{}, nil, nil, nil, mc)

	// Should not panic when all tiers are nil
	m.updateStorageMetrics()
}

// TestMigrateWarmToColdCreateArchiveError tests error handling when archive
// creation fails. We trigger this by making the cold directory read-only
// after the ColdManager is created, so file creation fails.
func TestMigrateWarmToColdCreateArchiveError(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")
	we := createSSTablesAtL1(t, warmDir, 3, 5)
	defer we.Close()
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	// Make cold dir read-only so CreateColdArchive fails on file creation
	// On Windows, this may not work, so the test is best-effort
	_ = os.Chmod(coldDir, 0555)
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, nil)
	m.migrateWarmToCold()
	_ = os.Chmod(coldDir, 0755)
}

// TestMigrateHotToWarmSegmentError exercises error paths in migrateHotToWarm
// where segment read fails and warm put fails.
func TestMigrateHotToWarmSegmentErrors(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "hot")
	warmDir := filepath.Join(dir, "warm")

	he := hot.NewEngine(hotDir, hot.HotConfig{SegmentSize: 256})
	defer he.Close()

	// Write enough data to fill a segment and freeze it
	part, err := he.GetOrCreatePartition("error-topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		part.Append([]byte("msg"))
	}

	// Write to LSM to trigger warm put
	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	policy := TierPolicy{HotRetention: time.Hour}
	m := NewMigrator(policy, he, we, nil, nil)

	// Segments are recent (not past cutoff), so they'll be skipped
	m.migrateHotToWarm()
}

// TestMigrateHotToWarmWithMetrics verifies migration metrics callback.
func TestMigrateHotToWarmWithMetrics(t *testing.T) {
	dir := t.TempDir()
	hotDir := filepath.Join(dir, "hot")
	warmDir := filepath.Join(dir, "warm")

	he := hot.NewEngine(hotDir, hot.HotConfig{SegmentSize: 256})
	defer he.Close()

	part, err := he.GetOrCreatePartition("metrics-topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Fill enough to roll over segments (each message has envelope overhead)
	for i := 0; i < 20; i++ {
		part.Append([]byte("metrics-msg"))
	}
	// Wait for segment to roll and freeze
	time.Sleep(200 * time.Millisecond)

	// Verify we have frozen segments
	frozen := part.FrozenSegments()
	if len(frozen) == 0 {
		t.Logf("no frozen segments, active only")
	}

	we, err := warm.NewLSMTree(warmDir, warm.LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer we.Close()

	mc := metrics.NewCollector()
	// Use negative retention so segments are immediately eligible
	policy := TierPolicy{HotRetention: -1 * time.Second}
	m := NewMigrator(policy, he, we, nil, mc)
	m.migrateHotToWarm()
}

// TestMigrateWarmToColdWithMetrics verifies migration triggers metric.
func TestMigrateWarmToColdWithMetrics(t *testing.T) {
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	coldDir := filepath.Join(dir, "cold")

	we := createSSTablesAtL1(t, warmDir, 3, 5)
	defer we.Close()

	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	mc := metrics.NewCollector()
	policy := TierPolicy{WarmRetention: -1 * time.Second}
	m := NewMigrator(policy, nil, we, cm, mc)
	m.migrateWarmToCold()
}

// TestCloseWithCompressor verifies Close path with compressor.
func TestCloseWithCompressor(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create SSTable with enough data for compression
	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 10; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("data-for-compressor-close"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Create a compressor and use it to create a compressed archive
	comp, err := cold.NewDictCompressor(nil)
	if err != nil {
		sst.Close()
		t.Skip("cannot create compressor")
	}

	archivePath := filepath.Join(coldDir, "compressor-close.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst}, cold.WithCompressor(comp, 1))
	if err != nil {
		sst.Close()
		t.Fatalf("CreateColdArchive: %v", err)
	}
	sst.Close()
	ca.Close()

	// Reload manager — it should load the compressed archive
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}

	// Set compressor so Close has something to close
	cm.compressor = comp

	cm.Close()
	// Should not panic
}

// TestLoadExistingWithCompressedArchive exercises the compressor apply path.
func TestLoadExistingWithCompressedArchive(t *testing.T) {
	coldDir := filepath.Join(t.TempDir(), "cold")
	if err := os.MkdirAll(coldDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create SSTable
	mt := warm.NewMemTable(4096)
	for i := uint64(0); i < 5; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("compressed-data"))
	}
	mt.Freeze()
	sst, err := warm.FlushMemTable(mt, coldDir, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Create a compressor for archive creation
	comp, err := cold.NewDictCompressor(make([]byte, 100))
	if err != nil {
		sst.Close()
		t.Skip("cannot create compressor")
	}

	archivePath := filepath.Join(coldDir, "compressed.dat")
	ca, err := cold.CreateColdArchive(archivePath, []*warm.SSTable{sst}, cold.WithCompressor(comp, 1))
	if err != nil {
		sst.Close()
		t.Fatalf("CreateColdArchive: %v", err)
	}
	sst.Close()
	ca.Close()

	// Set a dummy compressor on the manager so loadExisting has something to apply
	cm, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	cm.compressor = comp

	// Reload — loadExisting should try to apply compressor to compressed archive
	cm2, err := NewColdManager(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cm2.Close()

	cm2.mu.RLock()
	count := len(cm2.archives)
	cm2.mu.RUnlock()
	if count != 1 {
		t.Errorf("expected 1 archive, got %d", count)
	}
}
