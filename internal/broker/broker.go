package broker

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chimeramq/chimera/internal/audit"
	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/cluster/geo"
	"github.com/chimeramq/chimera/internal/engine/dlq"
	"github.com/chimeramq/chimera/internal/engine/exchange"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/engine/ttl"
	"github.com/chimeramq/chimera/internal/fips"
	"github.com/chimeramq/chimera/internal/flow"
	"github.com/chimeramq/chimera/internal/idempotent"
	"github.com/chimeramq/chimera/internal/metrics"
	"github.com/chimeramq/chimera/internal/processing"
	"github.com/chimeramq/chimera/internal/schema"
	"github.com/chimeramq/chimera/internal/storage/encrypt"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/tier"
	"github.com/chimeramq/chimera/internal/storage/wal"
	"github.com/chimeramq/chimera/internal/storage/warm"
	"github.com/chimeramq/chimera/internal/tenant"
	"github.com/chimeramq/chimera/internal/tracing"
	"github.com/chimeramq/chimera/internal/wasm"

	clusterpkg "github.com/chimeramq/chimera/internal/cluster"
	"gopkg.in/yaml.v3"
)

// Broker is the central orchestrator for all ChimeraMQ components.
type Broker struct {
	mu            sync.Mutex // guards config updates
	config        *Config
	logger        *Logger
	wal           *wal.WAL
	storage       *hot.Engine
	topics        *TopicManager
	queueEngine   *queue.Engine
	streamEngine  *stream.Engine
	metrics       *metrics.Collector
	cluster       *clusterpkg.Manager
	encryptor     *encrypt.Encryptor
	authProvider  auth.AuthProvider
	aclEngine     *auth.ACLEngine
	otelTracer    *tracing.Tracer
	warmEngine    *warm.LSMTree
	coldMgr       *tier.ColdManager
	migrator      *tier.Migrator
	schemaReg     *schema.Registry
	schemaEnf     *schema.Enforcer
	ttlExpirer    *ttl.Expirer
	wasmRT        *wasm.Runtime
	processor     *processing.Processor
	dlqH          *dlq.DLQ
	flowCtrl      *flow.Controller
	deduper       *idempotent.Deduper
	tenantMgr     *tenant.Manager
	quotaEnforcer *tenant.ResourceQuotaEnforcer
	exchanges     *exchange.Registry
	handoff       *HandoffManager
	auditLogger   *audit.Logger
	geoManager    *geo.Manager
	mainListener  net.Listener
	protocolMux   interface{ Stop() }

	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
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

	// Step 0: Initialize FIPS mode (must be first)
	if b.config.FIPS.Enabled {
		fipsCfg := fips.Config{
			Enabled:             b.config.FIPS.Enabled,
			StrictMode:          b.config.FIPS.StrictMode,
			AllowedCipherSuites: b.config.FIPS.AllowedCipherSuites,
			MinTLSVersion:       b.config.FIPS.MinTLSVersion,
		}
		if err := fips.Initialize(fipsCfg); err != nil {
			return fmt.Errorf("FIPS initialization failed: %w", err)
		}
		if fips.ComplianceError() != nil {
			// Non-fatal warning will be logged after logger is initialized
			_ = fips.ComplianceError() // Acknowledge the error
		}
	}

	// Step 1: Logger
	b.logger = NewLogger(b.config.Logging)
	b.exchanges = exchange.NewRegistry()

	// Log FIPS mode status
	if b.config.FIPS.Enabled {
		if fips.IsEnabled() {
			b.logger.Info("FIPS 140-2 compliance mode enabled")
			if err := fips.ComplianceError(); err != nil {
				b.logger.Warn("FIPS system validation warning", "error", err)
			}
		} else {
			b.logger.Error("FIPS mode requested but could not be enabled")
		}
	}

	// Step 2b: Audit Logger (if enabled)
	if b.config.Audit.Enabled {
		auditCfg := audit.Config{
			Enabled:  true,
			LogPath:  b.config.Audit.LogPath,
			MaxSize:  b.config.Audit.MaxSize,
			MaxAge:   ParseDuration(b.config.Audit.MaxAge, 30*24*time.Hour),
			ToStdout: b.config.Audit.ToStdout,
		}
		var err error
		b.auditLogger, err = audit.NewLogger(auditCfg)
		if err != nil {
			b.logger.Warn("audit logger failed to start", "error", err)
		} else {
			b.logger.Info("audit logging enabled")
		}
	}

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
	} else {
		b.logger.Warn("authentication is DISABLED - all connections accepted without credentials")
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
	if b.config.Priority.Enabled {
		b.queueEngine.SetPriorityEnabled(true)
	}
	// Step 9: Stream Engine
	offsetStore, err := stream.NewOffsetStore(b.config.Node.DataDir)
	if err != nil {
		return fmt.Errorf("offset store init: %w", err)
	}
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
		b.migrator = tier.NewMigrator(tp, b.storage, b.warmEngine, b.coldMgr, b.metrics)
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

	// Step 16b: Geo-Replication Manager (if enabled)
	if b.config.GeoReplication.Enabled {
		geoCfg := geo.Config{
			Enabled:   true,
			LocalDC:   b.config.GeoReplication.LocalDC,
			BatchSize: b.config.GeoReplication.BatchSize,
		}
		if b.config.GeoReplication.ReplicationMode == "sync" {
			geoCfg.ReplicationMode = geo.ReplicationSync
		} else {
			geoCfg.ReplicationMode = geo.ReplicationAsync
		}
		if geoCfg.BatchSize == 0 {
			geoCfg.BatchSize = 100
		}
		geoCfg.FlushInterval = ParseDuration(b.config.GeoReplication.FlushInterval, time.Second)
		geoCfg.MaxLag = ParseDuration(b.config.GeoReplication.MaxLag, 30*time.Second)
		geoCfg.RetryPolicy = geo.RetryPolicy{
			MaxRetries:        10,
			InitialBackoff:    time.Second,
			MaxBackoff:        5 * time.Minute,
			BackoffMultiplier: 2.0,
		}

		// Convert remote DC configs
		for _, dc := range b.config.GeoReplication.RemoteDCs {
			geoCfg.RemoteDCs = append(geoCfg.RemoteDCs, geo.RemoteDCConfig{
				ID:            dc.ID,
				Name:          dc.Name,
				Address:       dc.Address,
				Region:        dc.Region,
				Topics:        dc.Topics,
				ExcludeTopics: dc.ExcludeTopics,
			})
		}

		b.geoManager = geo.NewManager(geoCfg)
		if err := b.geoManager.Start(); err != nil {
			return fmt.Errorf("geo-replication manager: %w", err)
		}
		b.logger.Info("geo-replication enabled",
			"mode", b.config.GeoReplication.ReplicationMode,
			"remote_dcs", len(geoCfg.RemoteDCs),
		)
	}

	// Step 17: DLQ (if enabled)
	if b.config.DLQ.Enabled {
		var err error
		b.dlqH, err = dlq.NewDLQ(dlq.Config{
			Enabled:     true,
			TopicPrefix: b.config.DLQ.TopicPrefix,
			MaxRetries:  b.config.DLQ.MaxRetries,
			DataDir:     filepath.Join(b.config.Node.DataDir, "dlq"),
		})
		if err != nil {
			return fmt.Errorf("DLQ init: %w", err)
		}
		b.logger.Info("DLQ enabled", "prefix", b.config.DLQ.TopicPrefix, "max_retries", b.config.DLQ.MaxRetries)
	}

	// Step 18: Flow Control (if enabled)
	if b.config.FlowControl.Enabled {
		b.flowCtrl = flow.NewController(flow.Config{
			Enabled:         true,
			MaxMemoryBytes:  b.config.FlowControl.MaxMemoryBytes,
			HighWatermark:   b.config.FlowControl.HighWatermark,
			MaxConnections:  b.config.FlowControl.MaxConnections,
			GlobalRateLimit: b.config.FlowControl.GlobalRateLimit,
			SlowConsumerTTL: ParseDuration(b.config.FlowControl.SlowConsumerTTL, 30*time.Second),
			MaxSlowTicks:    b.config.FlowControl.MaxSlowTicks,
		})
		b.logger.Info("flow control enabled")
	}

	// Step 18b: WASM Runtime (if enabled)
	if b.config.WASM.Enabled {
		modulesDir := b.config.WASM.ModulesDir
		if modulesDir == "" {
			modulesDir = filepath.Join(b.config.Node.DataDir, "wasm")
		}
		if err := os.MkdirAll(modulesDir, 0750); err != nil {
			return fmt.Errorf("create wasm dir: %w", err)
		}
		execTimeout := ParseDuration(b.config.WASM.ExecutionTimeout, 100*time.Millisecond)
		wasmCfg := wasm.RuntimeConfig{
			MaxMemoryPages:   b.config.WASM.MaxMemoryPages,
			ExecutionTimeout: execTimeout,
			ModulePoolSize:   b.config.WASM.ModulePoolSize,
			ModulesDir:       modulesDir,
		}
		if wasmCfg.MaxMemoryPages == 0 {
			wasmCfg.MaxMemoryPages = 256
		}
		if wasmCfg.ModulePoolSize == 0 {
			wasmCfg.ModulePoolSize = 4
		}
		b.wasmRT = wasm.NewRuntime(wasmCfg)
		b.logger.Info("WASM runtime enabled", "modules_dir", modulesDir)
	}

	// Step 18c: Stream Processor (if enabled)
	if b.config.Processing.Enabled {
		stateDir := b.config.Processing.StateDir
		if stateDir == "" {
			stateDir = filepath.Join(b.config.Node.DataDir, "state")
		}
		if err := os.MkdirAll(stateDir, 0750); err != nil {
			return fmt.Errorf("create state dir: %w", err)
		}
		b.processor = processing.NewProcessor(stateDir)
		b.processor.SetBroker(&brokerAPIAdapter{broker: b})
		b.logger.Info("stream processor enabled", "state_dir", stateDir)
	}

	// Step 18d: TTL Expirer topic registration
	if b.ttlExpirer != nil {
		for _, tc := range b.topics.ListTopics() {
			if tc.DefaultTTL > 0 {
				b.ttlExpirer.SetTopicConfig(tc.Name, &ttl.TopicTTLConfig{
					DefaultTTL: tc.DefaultTTL,
				})
			}
		}
	}

	// Step 19: Idempotent Producer (if enabled)
	if b.config.Idempotent.Enabled {
		b.deduper = idempotent.NewDeduper(idempotent.Config{
			Enabled:    true,
			WindowSize: ParseDuration(b.config.Idempotent.WindowSize, 5*time.Minute),
			MaxEntries: b.config.Idempotent.MaxEntries,
		})
		b.logger.Info("idempotent producer enabled")
	}

	// Step 19b: Multi-tenancy Manager (if enabled)
	if b.config.Tenant.Enabled {
		tenantCfg := tenant.Config{
			Enabled:   true,
			Separator: b.config.Tenant.Separator,
		}
		for _, tc := range b.config.Tenant.Tenants {
			tenantCfg.Tenants = append(tenantCfg.Tenants, tenant.TenantConfig{
				ID:          tc.ID,
				TopicPrefix: tc.TopicPrefix,
				Quotas: tenant.QuotaConfig{
					MaxTopics:       tc.Quotas.MaxTopics,
					MaxPartitions:   tc.Quotas.MaxPartitions,
					MaxPublishRate:  tc.Quotas.MaxPublishRate,
					MaxFetchRate:    tc.Quotas.MaxFetchRate,
					MaxConnections:  tc.Quotas.MaxConnections,
					MaxStorageBytes: tc.Quotas.MaxStorageBytes,
				},
				Metadata: tc.Metadata,
			})
		}
		b.tenantMgr = tenant.NewManager(tenantCfg)
		b.logger.Info("tenant manager enabled", "tenants", len(tenantCfg.Tenants))
	}

	// Step 19c: Multi-tenancy Quota Enforcer (if enabled)
	if b.config.Tenant.Enabled && b.tenantMgr != nil {
		b.quotaEnforcer = tenant.NewResourceQuotaEnforcer(b.tenantMgr)
		b.quotaEnforcer.Start()
		b.logger.Info("tenant quota enforcer started")
	}

	// Step 20: Handoff Manager (for rolling upgrades)
	if b.config.Node.HandoffEnabled {
		b.handoff = NewHandoffManager(b)
		if err := b.handoff.Start(); err != nil {
			b.logger.Warn("handoff manager failed to start", "error", err)
		} else {
			b.logger.Info("handoff manager enabled for rolling upgrades")
		}
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

	// Stop geo-replication manager
	if b.geoManager != nil {
		b.geoManager.Stop()
		b.logger.Info("geo-replication manager stopped")
	}

	// Stop quota enforcer
	if b.quotaEnforcer != nil {
		b.quotaEnforcer.Stop()
		b.logger.Info("quota enforcer stopped")
	}

	// Stop handoff manager first (signal we're shutting down)
	if b.handoff != nil {
		b.handoff.Stop()
		b.logger.Info("handoff manager stopped")
	}

	// Close audit logger
	if b.auditLogger != nil {
		_ = b.auditLogger.Close()
		b.logger.Info("audit logger closed")
	}

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
	_ = b.wal.Checkpoint(b.wal.Offset())
	b.wal.Close()
	b.logger.Info("WAL closed")

	// Close storage
	b.storage.Close()
	b.logger.Info("storage closed")

	// Shutdown tracer
	if b.otelTracer != nil {
		_ = b.otelTracer.Shutdown(context.Background())
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

// ReloadConfig reloads configuration from file and applies dynamic changes.
// Only certain settings can be changed at runtime (logging, limits, ACL, auth file).
func (b *Broker) ReloadConfig(configPath string) error {
	if b.logger != nil {
		b.logger.Info("reloading configuration", "path", configPath)
	}

	// Load new config from file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	newConfig := defaultConfig()
	if err := yaml.Unmarshal(data, newConfig); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	// Apply dynamic changes (only certain fields can be updated)
	b.applyDynamicConfig(newConfig)

	if b.logger != nil {
		b.logger.Info("configuration reloaded successfully")
	}
	return nil
}

// applyDynamicConfig applies configuration changes that don't require restart.
func (b *Broker) applyDynamicConfig(newCfg *Config) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update logging level dynamically
	if b.logger != nil && newCfg.Logging.Level != b.config.Logging.Level {
		b.logger.SetLevel(newCfg.Logging.Level)
		b.logger.Info("logging level updated", "new_level", newCfg.Logging.Level)
	}

	// Update logging format dynamically
	if b.logger != nil && newCfg.Logging.Format != b.config.Logging.Format {
		b.logger.SetFormat(newCfg.Logging.Format)
		b.logger.Info("logging format updated", "new_format", newCfg.Logging.Format)
	}

	// Reload auth file if changed
	if b.config.Auth.Enabled && b.config.Auth.Type == "file" {
		if fp, ok := b.authProvider.(*auth.FileProvider); ok {
			if err := fp.Reload(); err != nil {
				b.logger.Error("failed to reload auth file", "error", err)
			} else {
				b.logger.Info("auth file reloaded")
			}
		}
	}

	// Update limits (connection limits)
	if newCfg.Limits.MaxConnections != b.config.Limits.MaxConnections {
		b.config.Limits.MaxConnections = newCfg.Limits.MaxConnections
		b.logger.Info("connection limit updated", "new_limit", newCfg.Limits.MaxConnections)
	}

	// Update ACL entries
	if b.aclEngine != nil && newCfg.ACL.Enabled {
		entries := make([]auth.ACLEntry, len(newCfg.ACL.Entries))
		for i, e := range newCfg.ACL.Entries {
			entries[i] = auth.ACLEntry{
				Principal:    e.Principal,
				ResourceType: auth.ParseResourceType(e.Resource),
				ResourceName: e.Name,
				Operation:    auth.ParseOperation(e.Operation),
				Permission:   auth.ParsePermission(e.Permission),
			}
		}
		b.aclEngine.SetEntries(entries)
		b.config.ACL.Entries = newCfg.ACL.Entries
		b.logger.Info("ACL entries updated", "count", len(entries))
	}

	// Update flow control settings
	if b.flowCtrl != nil {
		if newCfg.FlowControl.Enabled != b.config.FlowControl.Enabled {
			b.config.FlowControl.Enabled = newCfg.FlowControl.Enabled
			b.logger.Info("flow control enabled state updated", "enabled", newCfg.FlowControl.Enabled)
		}
		if newCfg.FlowControl.MaxMemoryBytes != b.config.FlowControl.MaxMemoryBytes {
			b.config.FlowControl.MaxMemoryBytes = newCfg.FlowControl.MaxMemoryBytes
			b.logger.Info("flow control max memory updated", "value", newCfg.FlowControl.MaxMemoryBytes)
		}
	}
}

// StartConfigWatcher starts a background goroutine that watches the config file
// for changes and reloads it automatically.
func (b *Broker) StartConfigWatcher(configPath string, interval time.Duration) {
	if interval == 0 {
		interval = 30 * time.Second // default check interval
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var lastModTime time.Time
		if info, err := os.Stat(configPath); err == nil {
			lastModTime = info.ModTime()
		}

		for {
			select {
			case <-b.ctx.Done():
				return
			case <-ticker.C:
				info, err := os.Stat(configPath)
				if err != nil {
					continue
				}

				if info.ModTime().After(lastModTime) {
					lastModTime = info.ModTime()
					if err := b.ReloadConfig(configPath); err != nil {
						if b.logger != nil {
							b.logger.Error("config reload failed", "error", err)
						}
					}
				}
			}
		}
	}()

	if b.logger != nil {
		b.logger.Info("config watcher started", "path", configPath, "interval", interval)
	}
}

// Tracer returns the OpenTelemetry tracer (nil if tracing disabled).
func (b *Broker) Tracer() *tracing.Tracer { return b.otelTracer }

// ACLEngine returns the ACL engine (nil if ACL disabled).
func (b *Broker) ACLEngine() *auth.ACLEngine { return b.aclEngine }

// DLQHandler returns the DLQ handler (nil if DLQ disabled).
func (b *Broker) DLQHandler() *dlq.DLQ { return b.dlqH }

// FlowController returns the flow controller (nil if flow control disabled).
func (b *Broker) FlowController() *flow.Controller { return b.flowCtrl }

// Deduper returns the idempotent deduper (nil if idempotent disabled).
func (b *Broker) Deduper() *idempotent.Deduper { return b.deduper }

// TenantManager returns the tenant manager (nil if multi-tenancy disabled).
func (b *Broker) TenantManager() *tenant.Manager { return b.tenantMgr }

// QuotaEnforcer returns the resource quota enforcer (nil if multi-tenancy disabled).
func (b *Broker) QuotaEnforcer() *tenant.ResourceQuotaEnforcer { return b.quotaEnforcer }

// Exchanges returns the exchange registry.
func (b *Broker) Exchanges() *exchange.Registry { return b.exchanges }

// GeoManager returns the geo-replication manager (nil if disabled).
func (b *Broker) GeoManager() *geo.Manager { return b.geoManager }

// IsFIPSEnabled returns true if FIPS 140-2 mode is enabled.
func (b *Broker) IsFIPSEnabled() bool { return fips.IsEnabled() }

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
	_ = f.Sync()
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
