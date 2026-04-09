package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/metrics"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

// Broker is the central orchestrator for all ChimeraMQ components.
type Broker struct {
	config       *Config
	logger       *Logger
	wal          *wal.WAL
	storage      *hot.Engine
	topics       *TopicManager
	queueEngine  *queue.Engine
	streamEngine *stream.Engine
	metrics      *metrics.Collector

	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	lockFile  *os.File
	stopped   atomic.Bool
}

// NewBroker creates a new broker instance.
func NewBroker(cfg *Config) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
		metrics: metrics.NewCollector(),
	}
	return b, nil
}

// Start executes the bootstrap sequence.
func (b *Broker) Start() error {
	b.startTime = time.Now()

	// Step 2: Logger
	b.logger = NewLogger(b.config.Logging)

	// Step 3: Data directory
	if err := os.MkdirAll(b.config.Node.DataDir, 0750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// Step 5: Lock file
	var err error
	b.lockFile, err = acquireLockFile(b.config.Node.DataDir)
	if err != nil {
		return err
	}

	// Step 4: WAL
	walSyncMode := parseSyncMode(b.config.Storage.WAL.SyncMode)
	walSyncInterval := ParseDuration(b.config.Storage.WAL.SyncInterval, 100*time.Millisecond)
	b.wal, err = wal.NewWAL(
		filepath.Join(b.config.Node.DataDir, "wal"),
		b.config.Storage.WAL.MaxSize,
		walSyncMode,
		walSyncInterval,
	)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}

	// Step 5: Hot Storage
	b.storage = hot.NewEngine(
		b.config.Node.DataDir,
		hot.HotConfig{SegmentSize: b.config.Storage.Hot.SegmentSize},
	)

	// Step 7: Topic Manager
	b.topics, err = NewTopicManager(b.config.Node.DataDir, b.storage, b.wal, b.config.Limits)
	if err != nil {
		return fmt.Errorf("topic manager: %w", err)
	}

	// Step 8: Queue Engine
	b.queueEngine = queue.NewEngine()

	// Step 9: Stream Engine
	offsetStore := stream.NewOffsetStore(b.config.Node.DataDir)
	b.streamEngine = stream.NewEngine(b.storage, offsetStore)

	b.logger.Info("ChimeraMQ broker started",
		"node", b.config.Node.Name,
		"port", b.config.Listener.Port,
		"admin", b.config.Listener.AdminPort,
	)

	return nil
}

// Stop performs graceful shutdown in reverse order.
func (b *Broker) Stop() error {
	if !b.stopped.CompareAndSwap(false, true) {
		return nil // already stopped
	}
	b.logger.Info("initiating graceful shutdown")

	b.cancel()
	b.wg.Wait()

	// Stop engines (kills background goroutines)
	b.queueEngine.Close()
	b.streamEngine.Close()
	b.logger.Info("engines stopped")

	// Flush storage
	b.storage.FlushAll()
	b.logger.Info("storage flushed")

	// Checkpoint WAL
	b.wal.Checkpoint(b.wal.Offset())
	b.wal.Close()
	b.logger.Info("WAL closed")

	// Close storage
	b.storage.Close()
	b.logger.Info("storage closed")

	// Release lock
	if b.lockFile != nil {
		lockPath := b.lockFile.Name()
		b.lockFile.Close()
		os.Remove(lockPath)
	}

	b.logger.Info("shutdown complete")
	return nil
}

// Config returns the broker configuration.
func (b *Broker) Config() *Config { return b.config }

// Topics returns the topic manager.
func (b *Broker) Topics() *TopicManager { return b.topics }

// Storage returns the storage engine.
func (b *Broker) Storage() *hot.Engine { return b.storage }

// QueueEngine returns the queue engine.
func (b *Broker) QueueEngine() *queue.Engine { return b.queueEngine }

// StreamEngine returns the stream engine.
func (b *Broker) StreamEngine() *stream.Engine { return b.streamEngine }

// Metrics returns the metrics collector.
func (b *Broker) Metrics() *metrics.Collector { return b.metrics }

// StartTime returns when the broker started.
func (b *Broker) StartTime() time.Time { return b.startTime }

// Logger returns the broker logger.
func (b *Broker) Logger() *Logger { return b.logger }

func acquireLockFile(dataDir string) (*os.File, error) {
	lockPath := filepath.Join(dataDir, "chimera.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		if os.IsExist(err) {
			// Stale lock detection: read PID and check if the process is alive
			data, readErr := os.ReadFile(lockPath)
			if readErr != nil {
				return nil, fmt.Errorf("data directory locked (cannot read lock file)")
			}
			var pid int
			n, sErr := fmt.Sscanf(string(data), "%d", &pid)
			if sErr != nil || n != 1 {
				// Corrupt lock file — treat as stale
			} else if pid > 0 && isProcessAlive(pid) {
				return nil, fmt.Errorf("data directory locked by process %d", pid)
			}
			// Stale or corrupt lock — remove and reclaim
			os.Remove(lockPath)
			f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if err != nil {
				return nil, fmt.Errorf("data directory locked by another process")
			}
		} else {
			return nil, err
		}
	}
	fmt.Fprintf(f, "%d\n", os.Getpid())
	f.Sync()
	return f, nil
}

func parseSyncMode(s string) wal.SyncMode {
	switch s {
	case "immediate":
		return wal.SyncImmediate
	case "interval":
		return wal.SyncInterval
	case "os":
		return wal.SyncOS
	default:
		return wal.SyncInterval
	}
}

// isProcessAlive checks whether a process with the given PID is still running.
func isProcessAlive(pid int) bool {
	if pid == os.Getpid() {
		return true
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal(0) checks liveness on Unix.
	// On Windows, FindProcess itself fails for non-existent PIDs.
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}
