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

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/engine/ttl"
	"github.com/chimeramq/chimera/internal/processing"
	"github.com/chimeramq/chimera/internal/schema"
	"github.com/chimeramq/chimera/internal/metrics"
	"github.com/chimeramq/chimera/internal/storage/encrypt"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/tier"
	"github.com/chimeramq/chimera/internal/storage/wal"
	"github.com/chimeramq/chimera/internal/storage/warm"
	"github.com/chimeramq/chimera/internal/tracing"
	"github.com/chimeramq/chimera/internal/wasm"

	clusterpkg "github.com/chimeramq/chimera/internal/cluster"
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
	cluster      *clusterpkg.Manager
	encryptor    *encrypt.Encryptor
	authProvider auth.AuthProvider
	aclEngine    *auth.ACLEngine
	otelTracer   *tracing.Tracer
	warmEngine   *warm.LSMTree
	coldMgr      *tier.ColdManager
	migrator     *tier.Migrator
	schemaReg   *schema.Registry
	schemaEnf   *schema.Enforcer
	ttlExpirer   *ttl.Expirer
	wasmRT       *wasm.Runtime
	processor    *processing.Processor

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

	// Step 2b: Auth Provider (if enabled)
	if b.config.Auth.Enabled {
		switch b.config.Auth.Type {
		case "static":
			b.authProvider = auth.NewStaticProvider(b.config.Auth.Users, b.config.Auth.Tokens)
			b.logger.Info("auth enabled (static)")
		case "file":
			var err error
			b.authProvider, err = auth.NewFileProvider(b.config.Auth.AuthFile)
			if err != nil {
				return fmt.Errorf("auth file provider: %w", err)
			}
			b.logger.Info("auth enabled (file)", "path", b.config.Auth.AuthFile)
		case "mtls":
			b.authProvider = auth.NewMTLSProvider()
			b.logger.Info("auth enabled (mtls)")
		case "oauth":
			var err error
			b.authProvider, err = auth.NewOAuthProvider(
				b.config.Auth.OAuth.Issuer,
				b.config.Auth.OAuth.ClientID,
				b.config.Auth.OAuth.Audience,
			)
			if err != nil {
				return fmt.Errorf("oauth provider: %w", err)
			}
			b.logger.Info("auth enabled (oauth)", "issuer", b.config.Auth.OAuth.Issuer)
		case "ldap":
			b.authProvider = auth.NewLDAPProvider(
				b.config.Auth.LDAP.URL,
				b.config.Auth.LDAP.BindDN,
				b.config.Auth.LDAP.BindPassword,
				b.config.Auth.LDAP.BaseDN,
				b.config.Auth.LDAP.Filter,
				b.config.Auth.LDAP.UseTLS,
			)
			b.logger.Info("auth enabled (ldap)", "url", b.config.Auth.LDAP.URL)
		default:
			b.logger.Info("auth enabled (" + b.config.Auth.Type + ")")
		}
	}

	// Step 2c: ACL Engine (if enabled)
	if b.config.ACL.Enabled {
		defaultPolicy := auth.ParsePermission(b.config.ACL.DefaultPolicy)
		b.aclEngine = auth.NewACLEngine(defaultPolicy)
		for _, e := range b.config.ACL.Entries {
			b.aclEngine.AddEntry(auth.ACLEntry{
				Principal:    e.Principal,
				ResourceType: auth.ParseResourceType(e.Resource),
				ResourceName: e.Name,
				Operation:    auth.ParseOperation(e.Operation),
				Permission:   auth.ParsePermission(e.Permission),
			})
		}
		b.logger.Info("ACL enabled", "default_policy", b.config.ACL.DefaultPolicy, "entries", len(b.config.ACL.Entries))
	}

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

	// Step 10: Encryption (if enabled)
	if b.config.Storage.Encryption.Enabled {
		keyPath := b.config.Storage.Encryption.KeyPath
		if keyPath == "" {
			keyPath = filepath.Join(b.config.Node.DataDir, "encrypt.key")
		}
		b.encryptor, err = encrypt.NewEncryptor(keyPath)
		if err != nil {
			return fmt.Errorf("encryption init: %w", err)
		}
		b.logger.Info("at-rest encryption enabled")
	}

	// Step 11: Warm Tier (LSM-Tree, if enabled)
	if b.config.Storage.Warm.Enabled {
		warmDir := filepath.Join(b.config.Node.DataDir, "warm")
		warmCfg := warm.DefaultLSMConfig()
		if b.config.Storage.Warm.BlockSize > 0 {
			warmCfg.BlockSize = b.config.Storage.Warm.BlockSize
		}
		if b.config.Storage.Warm.BloomFPRate > 0 {
			warmCfg.BloomFPRate = b.config.Storage.Warm.BloomFPRate
		}
		if b.config.Storage.Warm.MemTableCapacity > 0 {
			warmCfg.MemTableCapacity = b.config.Storage.Warm.MemTableCapacity
		}
		b.warmEngine, err = warm.NewLSMTree(warmDir, warmCfg)
		if err != nil {
			return fmt.Errorf("warm tier init: %w", err)
		}
		b.logger.Info("warm tier (LSM-Tree) enabled")
	}

	// Step 12: Cold Tier (if enabled)
	if b.config.Storage.Cold.Enabled {
		coldDir := filepath.Join(b.config.Node.DataDir, "cold")
		b.coldMgr, err = tier.NewColdManager(coldDir)
		if err != nil {
			return fmt.Errorf("cold tier init: %w", err)
		}
		b.logger.Info("cold tier enabled",
			"compression", b.config.Storage.Cold.Compression,
			"level", b.config.Storage.Cold.CompressionLevel,
		)
	}

	// Step 13: Tier Migrator (if warm or cold enabled)
	if b.warmEngine != nil || b.coldMgr != nil {
		tp := tier.TierPolicy{
			HotRetention:  ParseDuration(b.config.Storage.TierPolicy.HotRetention, 1*time.Hour),
			WarmRetention: ParseDuration(b.config.Storage.TierPolicy.WarmRetention, 24*time.Hour),
			ColdRetention: ParseDuration(b.config.Storage.TierPolicy.ColdRetention, 7*24*time.Hour),
			HotMaxSize:    b.config.Storage.TierPolicy.HotMaxSize,
			WarmMaxSize:   b.config.Storage.TierPolicy.WarmMaxSize,
		}
		b.migrator = tier.NewMigrator(tp, b.storage, b.warmEngine, b.coldMgr)
		b.migrator.Start()
		b.streamEngine.SetMigrator(b.migrator)
		b.logger.Info("tier migrator started")
	}

	// Step 14: Schema Registry (if enabled)
	if b.config.Schema.Enabled {
		schemasDir := filepath.Join(b.config.Node.DataDir, "schemas")
		b.schemaReg, err = schema.NewRegistry(schemasDir)
		if err != nil {
			return fmt.Errorf("schema registry init: %w", err)
		}
		b.schemaEnf = schema.NewEnforcer(b.schemaReg)
		b.logger.Info("schema registry enabled")
	}

	// Step 15: TTL Expirer (if enabled)
	if b.config.TTL.Enabled {
		b.ttlExpirer = ttl.NewExpirer(b.storage)
		b.ttlExpirer.Start()
		b.logger.Info("TTL expirer started")
	}

	// Step 16: Cluster Manager (if enabled)
	if b.config.Cluster.Enabled {
		clusterCfg := clusterpkg.ClusterConfig{
			NodeID:            fmt.Sprintf("node-%d", b.config.Node.ID),
			DataDir:           b.config.Node.DataDir,
			Peers:             b.config.Cluster.Raft.Peers,
			ElectionTimeout:   ParseDuration(b.config.Cluster.Raft.ElectionTimeout, 1*time.Second),
			HeartbeatInterval: ParseDuration(b.config.Cluster.Raft.HeartbeatInterval, 150*time.Millisecond),
			SnapshotInterval:  ParseDuration(b.config.Cluster.Raft.SnapshotInterval, 5*time.Minute),
			MaxLogEntries:     b.config.Cluster.Raft.MaxLogEntries,
			GossipBindPort:    b.config.Cluster.Gossip.BindPort,
			GossipSeeds:       b.config.Cluster.Gossip.Seeds,
			ProbeInterval:     ParseDuration(b.config.Cluster.Gossip.ProbeInterval, 1*time.Second),
			ProbeTimeout:      ParseDuration(b.config.Cluster.Gossip.ProbeTimeout, 500*time.Millisecond),
			IndirectNodes:     b.config.Cluster.Gossip.IndirectNodes,
			SuspicionTimeout:  ParseDuration(b.config.Cluster.Gossip.SuspicionTimeout, 5*time.Second),
			ReplicationFactor: b.config.Cluster.Replication.DefaultFactor,
			MinISR:            b.config.Cluster.Replication.MinISR,
			AckPolicy:         b.config.Cluster.Replication.AckPolicy,
			SyncMode:          b.config.Cluster.Replication.SyncMode,
			MaxLag:            b.config.Cluster.Replication.MaxLag,
		}

		b.cluster, err = clusterpkg.NewManager(clusterCfg)
		if err != nil {
			return fmt.Errorf("cluster manager: %w", err)
		}
		if err := b.cluster.Start(); err != nil {
			return fmt.Errorf("start cluster: %w", err)
		}
		b.logger.Info("cluster mode enabled")
	}

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

	// Stop TTL expirer
	if b.ttlExpirer != nil {
		b.ttlExpirer.Stop()
		b.logger.Info("TTL expirer stopped")
	}

	// Close schema registry
	if b.schemaReg != nil {
		b.schemaReg.Close()
		b.logger.Info("schema registry closed")
	}

	// Stop tier migrator
	if b.migrator != nil {
		b.migrator.Stop()
		b.logger.Info("tier migrator stopped")
	}

	// Close warm engine
	if b.warmEngine != nil {
		b.warmEngine.Close()
		b.logger.Info("warm tier closed")
	}

	// Close cold manager
	if b.coldMgr != nil {
		b.coldMgr.Close()
		b.logger.Info("cold tier closed")
	}

	// Stop cluster manager
	if b.cluster != nil {
		b.cluster.Stop()
		b.logger.Info("cluster stopped")
	}

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

	// Shutdown tracer
	if b.otelTracer != nil {
		b.otelTracer.Shutdown(context.Background())
		b.logger.Info("tracer shutdown")
	}

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

// Cluster returns the cluster manager (nil in single-node mode).
func (b *Broker) Cluster() *clusterpkg.Manager { return b.cluster }

// WarmEngine returns the warm tier LSM-Tree (nil if not enabled).
func (b *Broker) WarmEngine() *warm.LSMTree { return b.warmEngine }

// ColdManager returns the cold tier manager (nil if not enabled).
func (b *Broker) ColdManager() *tier.ColdManager { return b.coldMgr }

// Migrator returns the tier migrator (nil if not enabled).
func (b *Broker) Migrator() *tier.Migrator { return b.migrator }

// Encryptor returns the at-rest encryptor (nil if not enabled).
func (b *Broker) Encryptor() *encrypt.Encryptor { return b.encryptor }

// SchemaRegistry returns the schema registry (nil if not enabled).
func (b *Broker) SchemaRegistry() *schema.Registry { return b.schemaReg }

// SchemaEnforcer returns the schema enforcer (nil if not enabled).
func (b *Broker) SchemaEnforcer() *schema.Enforcer { return b.schemaEnf }

// TTLExpirer returns the TTL expirer (nil if not enabled).
func (b *Broker) TTLExpirer() *ttl.Expirer { return b.ttlExpirer }

// WASMRuntime returns the WASM runtime (nil if not enabled).
func (b *Broker) WASMRuntime() *wasm.Runtime { return b.wasmRT }

// Processor returns the stream processor (nil if not enabled).
func (b *Broker) Processor() *processing.Processor { return b.processor }

// AuthProvider returns the authentication provider (nil if auth disabled).
func (b *Broker) AuthProvider() auth.AuthProvider { return b.authProvider }

// Tracer returns the OpenTelemetry tracer (nil if tracing disabled).
func (b *Broker) Tracer() *tracing.Tracer { return b.otelTracer }

// ACLEngine returns the ACL engine (nil if ACL disabled).
func (b *Broker) ACLEngine() *auth.ACLEngine { return b.aclEngine }

// IsClustered returns true if clustering is enabled.
func (b *Broker) IsClustered() bool { return b.cluster != nil }

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
