package tier

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/storage/cold"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/warm"
)

// TierPolicy controls tier migration thresholds.
type TierPolicy struct {
	HotRetention  time.Duration
	WarmRetention time.Duration
	ColdRetention time.Duration
	HotMaxSize    int64
	WarmMaxSize   int64
}

// Migrator orchestrates data migration between storage tiers.
type Migrator struct {
	mu     sync.RWMutex
	policy TierPolicy
	hot    *hot.Engine
	warm   *warm.LSMTree
	cold   *ColdManager
	stopCh chan struct{}
	done   chan struct{}
}

// ColdManager manages cold archive files.
type ColdManager struct {
	mu       sync.RWMutex
	dir      string
	archives map[string]*cold.ColdArchive
}

// NewColdManager creates a cold archive manager.
func NewColdManager(dir string) (*ColdManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	cm := &ColdManager{
		dir:      dir,
		archives: make(map[string]*cold.ColdArchive),
	}
	cm.loadExisting()
	return cm, nil
}

func (cm *ColdManager) loadExisting() {
	entries, _ := os.ReadDir(cm.dir)
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".dat" {
			continue
		}
		path := filepath.Join(cm.dir, e.Name())
		ca, err := cold.OpenColdArchive(path)
		if err != nil {
			continue
		}
		cm.archives[path] = ca
	}
}

// NewMigrator creates a new tier migration orchestrator.
func NewMigrator(policy TierPolicy, hotEngine *hot.Engine, warmEngine *warm.LSMTree, coldMgr *ColdManager) *Migrator {
	return &Migrator{
		policy: policy,
		hot:    hotEngine,
		warm:   warmEngine,
		cold:   coldMgr,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Start begins background tier migration.
func (m *Migrator) Start() {
	go m.run()
}

// Stop shuts down the migrator.
func (m *Migrator) Stop() {
	close(m.stopCh)
	<-m.done
}

// Read performs a tier-aware read: hot → warm → cold.
func (m *Migrator) Read(topic string, partitionID uint32, offset uint64) ([]byte, error) {
	// Try hot tier first
	part, err := m.hot.GetOrCreatePartition(topic, partitionID)
	if err == nil {
		data, err := part.Read(offset)
		if err == nil {
			return data, nil
		}
	}

	// Try warm tier
	if m.warm != nil {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, offset)
		val, found, deleted := m.warm.Get(key)
		if found {
			if deleted {
				return nil, fmt.Errorf("offset %d is deleted", offset)
			}
			return val, nil
		}
	}

	// Try cold tier
	if m.cold != nil {
		m.cold.mu.RLock()
		defer m.cold.mu.RUnlock()
		for _, ca := range m.cold.archives {
			rng := ca.OffsetRange()
			if offset >= rng.Min && offset <= rng.Max {
				val, err := ca.Get(offset)
				if err == nil {
					return val, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("offset %d not found in any tier", offset)
}

func (m *Migrator) run() {
	defer close(m.done)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.migrateHotToWarm()
			m.migrateWarmToCold()
			m.purgeExpiredCold()
		}
	}
}

func (m *Migrator) migrateHotToWarm() {
	if m.warm == nil || m.policy.HotRetention == 0 {
		return
	}
	// This is a placeholder for the actual migration logic.
	// In production, this would iterate partitions, find frozen segments
	// older than HotRetention, read their data, write to warm LSM, then
	// delete the hot segment.
}

func (m *Migrator) migrateWarmToCold() {
	if m.cold == nil || m.policy.WarmRetention == 0 {
		return
	}
	// Placeholder: collect old SSTables from warm tier, create cold archive.
}

func (m *Migrator) purgeExpiredCold() {
	if m.cold == nil || m.policy.ColdRetention == 0 {
		return
	}

	m.cold.mu.Lock()
	defer m.cold.mu.Unlock()

	now := time.Now()
	for path, ca := range m.cold.archives {
		if now.Sub(ca.CreatedAt()) > m.policy.ColdRetention {
			ca.Remove()
			delete(m.cold.archives, path)
		}
	}
}

// CloseColdManager closes all cold archives.
func (cm *ColdManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, ca := range cm.archives {
		ca.Close()
	}
}
