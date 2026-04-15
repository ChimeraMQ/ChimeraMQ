package tier

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/metrics"
	"github.com/chimeramq/chimera/internal/storage/cold"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/warm"
	"log"
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
	policy  TierPolicy
	hot     *hot.Engine
	warm    *warm.LSMTree
	cold    *ColdManager
	metrics *metrics.Collector
	stopCh  chan struct{}
	done    chan struct{}
}

// ColdManager manages cold archive files.
type ColdManager struct {
	mu           sync.RWMutex
	dir          string
	archives     map[string]*cold.ColdArchive
	archiveCount int
	trainer      *cold.ZstdDictTrainer
	compressor   *cold.DictCompressor
	compressorMu sync.RWMutex
}

// NewColdManager creates a cold archive manager.
func NewColdManager(dir string) (*ColdManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	cm := &ColdManager{
		dir:      dir,
		archives: make(map[string]*cold.ColdArchive),
		trainer:  cold.NewZstdDictTrainer(dir),
	}
	cm.loadExisting()

	// Load trained dictionary if available
	if cm.trainer.HasDict() {
		dict, err := cm.trainer.LoadDict()
		if err == nil {
			comp, err := cold.NewDictCompressor(dict)
			if err == nil {
				cm.compressor = comp
				log.Printf("cold: loaded zstd dictionary from %s", cm.trainer.DictPath())
			}
		}
	}

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
		cm.archiveCount++

		// Apply compressor if archive is compressed and we have a dictionary
		if ca.IsCompressed() && cm.compressor != nil {
			ca.SetCompressor(cm.compressor)
		}
	}
}

// NewMigrator creates a new tier migration orchestrator.
func NewMigrator(policy TierPolicy, hotEngine *hot.Engine, warmEngine *warm.LSMTree, coldMgr *ColdManager, mc *metrics.Collector) *Migrator {
	return &Migrator{
		policy:  policy,
		hot:     hotEngine,
		warm:    warmEngine,
		cold:    coldMgr,
		metrics: mc,
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
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

	// Update metrics immediately on start
	m.updateStorageMetrics()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.migrateHotToWarm()
			m.migrateWarmToCold()
			m.purgeExpiredCold()
			m.updateStorageMetrics()
		}
	}
}

// updateStorageMetrics updates the storage size metrics for all tiers.
func (m *Migrator) updateStorageMetrics() {
	if m.metrics == nil {
		return
	}

	// Hot tier size
	if m.hot != nil {
		hotSize := m.hot.TotalSize()
		m.metrics.TierStorageBytes("hot", hotSize)
	}

	// Warm tier size
	if m.warm != nil {
		warmSize := m.warm.TotalSize()
		m.metrics.TierStorageBytes("warm", warmSize)
	}

	// Cold tier size
	if m.cold != nil {
		coldSize := m.cold.TotalSize()
		m.metrics.TierStorageBytes("cold", coldSize)
	}
}

func (m *Migrator) migrateHotToWarm() {
	if m.warm == nil || m.policy.HotRetention == 0 {
		return
	}

	cutoff := time.Now().Add(-m.policy.HotRetention)
	totalMigrated := 0

	m.hot.ForEachPartition(func(topic string, partID uint32, p *hot.Partition) bool {
		frozen := p.FrozenSegments()
		for _, seg := range frozen {
			if seg.Created().After(cutoff) {
				continue
			}

			baseOff := seg.BaseOffset()
			nextOff := seg.NextOffset()
			migrated := 0

			for off := baseOff; off < nextOff; off++ {
				data, err := p.Read(off)
				if err != nil {
					continue
				}

				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, off)
				if err := m.warm.Put(key, data); err != nil {
					log.Printf("tier: warm put offset %d: %v", off, err)
					continue
				}
				migrated++
			}

			if migrated > 0 {
				totalMigrated += migrated
				p.RemoveSegment(seg)
				if err := seg.Remove(); err != nil {
					log.Printf("tier: remove segment %s: %v", seg.Path(), err)
				}
				log.Printf("tier: migrated %d records from hot segment %s to warm", migrated, seg.Path())
			}
		}
		return true
	})

	if totalMigrated > 0 && m.metrics != nil {
		m.metrics.TierMigrationTotal("hot", "warm")
	}
}

func (m *Migrator) migrateWarmToCold() {
	if m.cold == nil || m.policy.WarmRetention == 0 {
		return
	}

	oldSSTs := m.warm.OldSSTables(m.policy.WarmRetention)
	if len(oldSSTs) == 0 {
		return
	}

	// Collect samples for dictionary training before archiving
	if m.cold.trainer != nil {
		m.cold.trainer.AddSamples(oldSSTs)
	}

	batchSize := 10
	archivedCount := 0
	for i := 0; i < len(oldSSTs); i += batchSize {
		end := i + batchSize
		if end > len(oldSSTs) {
			end = len(oldSSTs)
		}
		batch := oldSSTs[i:end]

		archiveName := fmt.Sprintf("cold-%d.dat", time.Now().UnixNano())
		archivePath := filepath.Join(m.cold.dir, archiveName)

		ca, err := cold.CreateColdArchive(archivePath, batch)
		if err != nil {
			log.Printf("tier: create cold archive: %v", err)
			continue
		}

		m.cold.mu.Lock()
		m.cold.archives[archivePath] = ca
		m.cold.archiveCount++
		m.cold.mu.Unlock()

		for _, sst := range batch {
			m.warm.RemoveSSTable(sst)
		}

		archivedCount += len(batch)
		log.Printf("tier: archived %d SSTables to %s", len(batch), archiveName)
	}

	// Check if dictionary training should trigger
	if archivedCount > 0 && m.cold.trainer != nil {
		m.cold.mu.RLock()
		count := m.cold.archiveCount
		m.cold.mu.RUnlock()

		if m.cold.trainer.ShouldTrain(count) {
			// Train may panic due to a bug in klauspost/compress zstd.BuildDict
			var dict []byte
			var trainErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						trainErr = fmt.Errorf("dictionary training panicked: %v", r)
					}
				}()
				dict, trainErr = m.cold.trainer.Train()
			}()
			if trainErr != nil {
				log.Printf("cold: dictionary training failed: %v", trainErr)
			} else {
				// Activate new compressor with trained dictionary
				m.cold.compressorMu.Lock()
				if m.cold.compressor != nil {
					m.cold.compressor.Close()
				}
				comp, err := cold.NewDictCompressor(dict)
				if err != nil {
					log.Printf("cold: new compressor failed: %v", err)
				} else {
					m.cold.compressor = comp
					// Apply to existing compressed archives
					m.cold.mu.RLock()
					for _, ca := range m.cold.archives {
						if ca.IsCompressed() {
							ca.SetCompressor(comp)
						}
					}
					m.cold.mu.RUnlock()
					log.Printf("cold: dictionary trained and activated (%d samples)", len(dict))
				}
				m.cold.compressorMu.Unlock()
			}
		}
	}

	if archivedCount > 0 && m.metrics != nil {
		m.metrics.TierMigrationTotal("warm", "cold")
	}
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
			_ = ca.Remove()
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
	if cm.compressor != nil {
		cm.compressor.Close()
	}
}

// TotalSize returns the total size of all cold archives in bytes.
func (cm *ColdManager) TotalSize() int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var total int64
	for _, ca := range cm.archives {
		total += ca.Size()
	}
	return total
}
