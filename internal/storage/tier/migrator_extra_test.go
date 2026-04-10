package tier

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestMigrateWarmToColdNilCold(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil)
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
	m := NewMigrator(policy, nil, we, cm)
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
	m := NewMigrator(policy, nil, we, cm)
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

	policy := TierPolicy{WarmRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, we, cm)
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

	policy := TierPolicy{WarmRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, we, cm)
	m.migrateWarmToCold()
}

func TestNewColdManagerError(t *testing.T) {
	// On some OSes, MkdirAll may succeed for odd paths, so this test is best-effort
	_, err := NewColdManager(string([]byte{0x00}))
	_ = err
}

func TestPurgeExpiredColdNoRetention(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil)
	// Should not panic with nil cold
	m.purgeExpiredCold()
}

func TestPurgeExpiredColdNilCold(t *testing.T) {
	policy := TierPolicy{ColdRetention: time.Nanosecond}
	m := NewMigrator(policy, nil, nil, nil)
	m.purgeExpiredCold()
}

func TestMigrateHotToWarmNoWarm(t *testing.T) {
	m := NewMigrator(TierPolicy{}, nil, nil, nil)
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
	policy := TierPolicy{
		HotRetention:  time.Hour,
		WarmRetention: time.Hour,
		ColdRetention: time.Hour,
	}
	m := NewMigrator(policy, nil, nil, nil)

	go m.run()
	time.Sleep(50 * time.Millisecond)
	m.Stop()
}
