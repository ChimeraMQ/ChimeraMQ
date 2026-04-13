package broker

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all ChimeraMQ broker configuration.
type Config struct {
	Node           NodeConfig           `yaml:"node"`
	Listener       ListenerConfig       `yaml:"listener"`
	Storage        StorageConfig        `yaml:"storage"`
	Defaults       DefaultsConfig       `yaml:"defaults"`
	Logging        LoggingConfig        `yaml:"logging"`
	Auth           AuthConfig           `yaml:"auth"`
	TLS            TLSConfig            `yaml:"tls"`
	Limits         LimitsConfig         `yaml:"limits"`
	Protocols      ProtocolsConfig      `yaml:"protocols"`
	Cluster        ClusterConfig        `yaml:"cluster"`
	Schema         SchemaConfig         `yaml:"schema"`
	Priority       PriorityConfig       `yaml:"priority"`
	TTL            TTLConfigRoot        `yaml:"ttl"`
	WASM           WASMConfig           `yaml:"wasm"`
	ACL            ACL                  `yaml:"acl"`
	Processing     ProcessingConfig     `yaml:"processing"`
	Observability  ObservabilityConfig  `yaml:"observability"`
	Audit          AuditConfig          `yaml:"audit"`
	DLQ            DLQConfig            `yaml:"dlq"`
	FlowControl    FlowControlConfig    `yaml:"flow_control"`
	Idempotent     IdempotentConfig     `yaml:"idempotent"`
	Tenant         TenantConfigRoot     `yaml:"tenant"`
	GeoReplication GeoReplicationConfig `yaml:"geo_replication"`
	FIPS           FIPSConfig           `yaml:"fips"`
}

// ClusterConfig controls clustering behavior.
type ClusterConfig struct {
	Enabled     bool              `yaml:"enabled"`
	Raft        RaftConfig        `yaml:"raft"`
	Gossip      GossipConfig      `yaml:"gossip"`
	Replication ReplicationConfig `yaml:"replication"`
}

// RaftConfig controls the Raft consensus layer.
type RaftConfig struct {
	Peers             []string `yaml:"peers"`
	ElectionTimeout   string   `yaml:"election_timeout"`
	HeartbeatInterval string   `yaml:"heartbeat_interval"`
	SnapshotInterval  string   `yaml:"snapshot_interval"`
	MaxLogEntries     int      `yaml:"max_log_entries"`
}

// GossipConfig controls the SWIM gossip layer.
type GossipConfig struct {
	BindPort         int      `yaml:"bind_port"`
	Seeds            []string `yaml:"seeds"`
	ProbeInterval    string   `yaml:"probe_interval"`
	ProbeTimeout     string   `yaml:"probe_timeout"`
	IndirectNodes    int      `yaml:"indirect_nodes"`
	SuspicionTimeout string   `yaml:"suspicion_timeout"`
}

// ReplicationConfig controls partition replication.
type ReplicationConfig struct {
	DefaultFactor int    `yaml:"default_factor"`
	MinISR        int    `yaml:"min_isr"`
	AckPolicy     string `yaml:"ack_policy"` // leader, quorum, all
	SyncMode      string `yaml:"sync_mode"`  // sync, async
	MaxLag        int64  `yaml:"max_lag"`
}

// ProtocolsConfig controls which protocol adapters are enabled.
type ProtocolsConfig struct {
	Chimera   ProtocolChimeraConfig   `yaml:"chimera"`
	MQTT      ProtocolMQTTConfig      `yaml:"mqtt"`
	WebSocket ProtocolWebSocketConfig `yaml:"websocket"`
	AMQP      ProtocolAMQPConfig      `yaml:"amqp"`
	STOMP     ProtocolSTOMPConfig     `yaml:"stomp"`
	NATS      ProtocolNATSConfig      `yaml:"nats"`
}

// ProtocolChimeraConfig controls the native Chimera protocol.
type ProtocolChimeraConfig struct {
	Enabled      bool  `yaml:"enabled"`
	MaxFrameSize int32 `yaml:"max_frame_size"`
}

// ProtocolMQTTConfig controls the MQTT adapter.
type ProtocolMQTTConfig struct {
	Enabled        bool   `yaml:"enabled"`
	MaxPacketSize  int32  `yaml:"max_packet_size"`
	MaxQoS         uint8  `yaml:"max_qos"`
	MaxTopicAlias  uint16 `yaml:"max_topic_alias"`
	RetainedMax    int    `yaml:"retained_max"`
	TopicSeparator string `yaml:"topic_separator"`
}

// ProtocolWebSocketConfig controls the WebSocket adapter.
type ProtocolWebSocketConfig struct {
	Enabled      bool   `yaml:"enabled"`
	Path         string `yaml:"path"`
	MaxFrameSize int64  `yaml:"max_frame_size"`
}

// ProtocolAMQPConfig controls the AMQP 1.0 adapter.
type ProtocolAMQPConfig struct {
	Enabled      bool  `yaml:"enabled"`
	MaxFrameSize int32 `yaml:"max_frame_size"`
}

// ProtocolSTOMPConfig controls the STOMP adapter.
type ProtocolSTOMPConfig struct {
	Enabled        bool `yaml:"enabled"`
	MaxFrameSize   int  `yaml:"max_frame_size"`
	HeartBeat      bool `yaml:"heartbeat"`
	MaxConnections int  `yaml:"max_connections"`
}

// ProtocolNATSConfig controls the NATS adapter.
type ProtocolNATSConfig struct {
	Enabled     bool `yaml:"enabled"`
	MaxPayload  int  `yaml:"max_payload"`
	MaxPending  int  `yaml:"max_pending"`
	PingTimeout int  `yaml:"ping_timeout"`
}

// TLSConfig controls TLS encryption for listeners.
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file,omitempty"`
	MinVersion string `yaml:"min_version,omitempty"`
	Mutual     bool   `yaml:"mutual,omitempty"`
	ClientCA   string `yaml:"client_ca,omitempty"`
}

// AuditConfig controls audit logging.
type AuditConfig struct {
	Enabled  bool   `yaml:"enabled"`
	LogPath  string `yaml:"log_path"`
	MaxSize  int64  `yaml:"max_size"` // bytes
	MaxAge   string `yaml:"max_age"`  // duration string
	ToStdout bool   `yaml:"to_stdout"`
}

// AuthConfig controls authentication.
type AuthConfig struct {
	Enabled  bool              `yaml:"enabled"`
	Type     string            `yaml:"type"` // static, file, oauth, ldap
	Users    map[string]string `yaml:"users"`
	AuthFile string            `yaml:"auth_file,omitempty"`
	Tokens   map[string]string `yaml:"tokens"`
	OAuth    OAuthConfig       `yaml:"oauth,omitempty"`
	LDAP     LDAPConfig        `yaml:"ldap,omitempty"`
}

// OAuthConfig controls OAuth 2.0 / OIDC authentication.
type OAuthConfig struct {
	Issuer       string   `yaml:"issuer"`
	ClientID     string   `yaml:"client_id"`
	ClientSecret string   `yaml:"client_secret"`
	Scopes       []string `yaml:"scopes"`
	Audience     string   `yaml:"audience"`
}

// LDAPConfig controls LDAP authentication.
type LDAPConfig struct {
	URL          string `yaml:"url"`
	BindDN       string `yaml:"bind_dn"`
	BindPassword string `yaml:"bind_password"`
	BaseDN       string `yaml:"base_dn"`
	Filter       string `yaml:"filter"`
	UseTLS       bool   `yaml:"tls"`
}

// ACLConfig controls access control lists.
type ACLConfig struct {
	Enabled       bool   `yaml:"enabled"`
	DefaultPolicy string `yaml:"default_policy"` // "allow" or "deny"
}

// ACL contains the parsed ACL entries for config.
type ACL struct {
	Enabled       bool             `yaml:"enabled"`
	DefaultPolicy string           `yaml:"default_policy"`
	Entries       []ACLEntryConfig `yaml:"entries"`
}

// ACLEntryConfig is one ACL entry in the config file.
type ACLEntryConfig struct {
	Principal  string `yaml:"principal"`
	Resource   string `yaml:"resource"`
	Name       string `yaml:"name"`
	Operation  string `yaml:"operation"`
	Permission string `yaml:"permission"`
}

// LimitsConfig controls resource caps.
type LimitsConfig struct {
	MaxPartitionsPerTopic uint32 `yaml:"max_partitions_per_topic"`
	MaxTopics             int    `yaml:"max_topics"`
	MaxFetchMessages      int    `yaml:"max_fetch_messages"`
	MaxMessageSize        int64  `yaml:"max_message_size"`
	MaxConnections        int64  `yaml:"max_connections"`
}

// NodeConfig identifies this broker node.
type NodeConfig struct {
	ID             uint64 `yaml:"id"`
	Name           string `yaml:"name"`
	DataDir        string `yaml:"data_dir"`
	HandoffEnabled bool   `yaml:"handoff_enabled"`
}

// ListenerConfig controls network listeners.
type ListenerConfig struct {
	Bind           string `yaml:"bind"`
	Port           int    `yaml:"port"`
	AdminPort      int    `yaml:"admin_port"`
	MaxConnections int    `yaml:"max_connections"`
}

// StorageConfig controls storage tiers.
type StorageConfig struct {
	Hot        HotConfig        `yaml:"hot"`
	WAL        WALConfig        `yaml:"wal"`
	Warm       WarmConfig       `yaml:"warm"`
	Cold       ColdConfig       `yaml:"cold"`
	TierPolicy TierPolicyConfig `yaml:"tier_policy"`
	Encryption EncryptionConfig `yaml:"encryption"`
}

// HotConfig controls hot tier (mmap) storage.
type HotConfig struct {
	SegmentSize  int64  `yaml:"segment_size"`
	SyncMode     string `yaml:"sync_mode"`
	SyncInterval string `yaml:"sync_interval"`
	MaxSegments  int    `yaml:"max_segments"`
}

// WALConfig controls the write-ahead log.
type WALConfig struct {
	MaxSize      int64  `yaml:"max_size"`
	SyncMode     string `yaml:"sync_mode"`
	SyncInterval string `yaml:"sync_interval"`
}

// TierPolicyConfig controls tier migration thresholds.
type TierPolicyConfig struct {
	HotRetention     string `yaml:"hot_retention"`
	WarmRetention    string `yaml:"warm_retention"`
	ColdRetention    string `yaml:"cold_retention"`
	HotMaxSize       int64  `yaml:"hot_max_size"`
	WarmMaxSize      int64  `yaml:"warm_max_size"`
	CompactOnMigrate bool   `yaml:"compact_on_migrate"`
}

// WarmConfig controls warm tier (LSM-Tree) storage.
type WarmConfig struct {
	Enabled            bool    `yaml:"enabled"`
	BlockSize          int     `yaml:"block_size"`
	BloomFPRate        float64 `yaml:"bloom_fp_rate"`
	CompactionStrategy string  `yaml:"compaction_strategy"`
	CompactionInterval string  `yaml:"compaction_interval"`
	MemTableCapacity   int64   `yaml:"memtable_capacity"`
}

// ColdConfig controls cold tier (compressed archive) storage.
type ColdConfig struct {
	Enabled              bool   `yaml:"enabled"`
	ArchiveSize          int64  `yaml:"archive_size"`
	Compression          string `yaml:"compression"`
	CompressionLevel     int    `yaml:"compression_level"`
	DictTrainingInterval int    `yaml:"dict_training_interval"`
}

// SchemaConfig controls the schema registry.
type SchemaConfig struct {
	Enabled       bool   `yaml:"enabled"`
	DefaultCompat string `yaml:"default_compat"` // none, backward, forward, full
}

// PriorityConfig controls priority queue behavior.
type PriorityConfig struct {
	Enabled bool `yaml:"enabled"`
	Levels  int  `yaml:"levels"` // 1-10, default 10
}

// TTLConfigRoot controls message TTL enforcement.
type TTLConfigRoot struct {
	Enabled       bool   `yaml:"enabled"`
	ScanInterval  string `yaml:"scan_interval"`  // default "1s"
	DefaultAction string `yaml:"default_action"` // drop or dlq
}

// WASMConfig controls the WASM runtime.
type WASMConfig struct {
	Enabled          bool   `yaml:"enabled"`
	MaxMemoryPages   uint32 `yaml:"max_memory_pages"`  // default 256 (16MB)
	ExecutionTimeout string `yaml:"execution_timeout"` // default "100ms"
	ModulePoolSize   int    `yaml:"module_pool_size"`  // default 4
	ModulesDir       string `yaml:"modules_dir"`       // default "{data_dir}/wasm"
}

// ProcessingConfig controls stream processing.
type ProcessingConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CheckpointInterval string `yaml:"checkpoint_interval"` // default "30s"
	StateDir           string `yaml:"state_dir"`           // default "{data_dir}/state"
}

// ObservabilityConfig controls tracing and monitoring.
type ObservabilityConfig struct {
	Tracing   TracingConfig   `yaml:"tracing"`
	Dashboard DashboardConfig `yaml:"dashboard"`
	PProf     PProfConfig     `yaml:"pprof"`
}

// PProfConfig controls profiling endpoints.
type PProfConfig struct {
	Enabled bool `yaml:"enabled"`
}

// DashboardConfig controls the embedded Web UI.
type DashboardConfig struct {
	Enabled bool `yaml:"enabled"`
}

// TracingConfig controls OpenTelemetry tracing.
type TracingConfig struct {
	Enabled    bool    `yaml:"enabled"`
	Endpoint   string  `yaml:"endpoint"`
	Service    string  `yaml:"service"`
	Insecure   bool    `yaml:"insecure"`
	SampleRate float64 `yaml:"sample_rate"`
}

// DLQConfig controls dead letter queue behavior.
type DLQConfig struct {
	Enabled     bool   `yaml:"enabled"`
	TopicPrefix string `yaml:"topic_prefix"` // default: "__dlq_"
	MaxRetries  int    `yaml:"max_retries"`  // default: 3
}

// FlowControlConfig controls backpressure and flow control.
type FlowControlConfig struct {
	Enabled         bool    `yaml:"enabled"`
	MaxMemoryBytes  int64   `yaml:"max_memory_bytes"`  // 0 = no limit
	HighWatermark   float64 `yaml:"high_watermark"`    // 0.0-1.0, default 0.85
	MaxConnections  int64   `yaml:"max_connections"`   // 0 = no limit
	GlobalRateLimit int64   `yaml:"global_rate_limit"` // msgs/sec, 0 = unlimited
	SlowConsumerTTL string  `yaml:"slow_consumer_ttl"` // default: "30s"
	MaxSlowTicks    int     `yaml:"max_slow_ticks"`    // default: 3
}

// IdempotentConfig controls idempotent producer behavior.
type IdempotentConfig struct {
	Enabled    bool   `yaml:"enabled"`
	WindowSize string `yaml:"window_size"` // default: "5m"
	MaxEntries int    `yaml:"max_entries"` // per producer, default: 10000
}

// TenantConfigRoot controls multi-tenancy.
type TenantConfigRoot struct {
	Enabled   bool              `yaml:"enabled"`
	Separator string            `yaml:"separator"`
	Tenants   []TenantConfigDef `yaml:"tenants"`
}

// TenantConfigDef is the YAML config for a single tenant.
type TenantConfigDef struct {
	ID          string            `yaml:"id"`
	TopicPrefix string            `yaml:"topic_prefix"`
	Quotas      TenantQuotaConfig `yaml:"quotas"`
	Metadata    map[string]string `yaml:"metadata"`
}

// TenantQuotaConfig holds per-tenant resource limits.
type TenantQuotaConfig struct {
	MaxTopics       int   `yaml:"max_topics"`
	MaxPartitions   int   `yaml:"max_partitions"`
	MaxPublishRate  int64 `yaml:"max_publish_rate"`  // msgs/sec, 0=unlimited
	MaxFetchRate    int64 `yaml:"max_fetch_rate"`    // fetches/sec, 0=unlimited
	MaxConnections  int64 `yaml:"max_connections"`   // 0=unlimited
	MaxStorageBytes int64 `yaml:"max_storage_bytes"` // 0=unlimited
}

// GeoReplicationConfig controls geo-replication behavior.
type GeoReplicationConfig struct {
	Enabled         bool                `yaml:"enabled"`
	LocalDC         string              `yaml:"local_dc"`
	RemoteDCs       []GeoRemoteDCConfig `yaml:"remote_dcs"`
	ReplicationMode string              `yaml:"replication_mode"` // async, sync
	BatchSize       int                 `yaml:"batch_size"`
	FlushInterval   string              `yaml:"flush_interval"`
	MaxLag          string              `yaml:"max_lag"`
}

// GeoRemoteDCConfig represents a remote datacenter configuration.
type GeoRemoteDCConfig struct {
	ID            string   `yaml:"id"`
	Name          string   `yaml:"name"`
	Address       string   `yaml:"address"`
	Region        string   `yaml:"region"`
	Topics        []string `yaml:"topics"`
	ExcludeTopics []string `yaml:"exclude_topics"`
}

// FIPSConfig controls FIPS 140-2 compliance mode.
type FIPSConfig struct {
	Enabled             bool     `yaml:"enabled"`
	StrictMode          bool     `yaml:"strict_mode"`
	AllowedCipherSuites []string `yaml:"allowed_cipher_suites"`
	MinTLSVersion       string   `yaml:"min_tls_version"`
}

// EncryptionConfig controls at-rest encryption.
type EncryptionConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Algorithm string `yaml:"algorithm"`
	KeyPath   string `yaml:"key_path"`
	KeyRotate string `yaml:"key_rotate"`
}

// DefaultsConfig holds default values for new topics.
type DefaultsConfig struct {
	Topic TopicDefaults `yaml:"topic"`
}

// TopicDefaults are applied when a topic is created without explicit settings.
type TopicDefaults struct {
	Partitions    uint32 `yaml:"partitions"`
	RetentionTime string `yaml:"retention_time"`
	Mode          string `yaml:"mode"`
	Replication   uint32 `yaml:"replication"`
}

// LoggingConfig controls structured logging.
type LoggingConfig struct {
	Level   string `yaml:"level"`
	Format  string `yaml:"format"`
	Output  string `yaml:"output"`
	File    string `yaml:"file"`
	MaxSize int64  `yaml:"max_size"` // bytes, 0 = no rotation
	MaxAge  int    `yaml:"max_age"`  // days, 0 = no age limit
}

// CLIFlags holds overrides from command-line flags.
type CLIFlags struct {
	DataDir   string
	Bind      string
	Port      int
	AdminPort int
	LogLevel  string
}

// LoadConfig loads configuration with priority: CLI > env > YAML > defaults.
func LoadConfig(configPath string, flags *CLIFlags) (*Config, error) {
	cfg := defaultConfig()

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("reading config: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parsing config: %w", err)
		}
	}

	applyEnvOverrides(cfg)

	if flags != nil {
		applyCLIOverrides(cfg, flags)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func defaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:      1,
			Name:    "chimera-01",
			DataDir: "/var/lib/chimera",
		},
		Listener: ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           5672,
			AdminPort:      9090,
			MaxConnections: 100000,
		},
		Storage: StorageConfig{
			Hot: HotConfig{
				SegmentSize:  256 * 1024 * 1024,
				SyncMode:     "interval",
				SyncInterval: "200ms",
				MaxSegments:  10,
			},
			WAL: WALConfig{
				MaxSize:      128 * 1024 * 1024,
				SyncMode:     "interval",
				SyncInterval: "100ms",
			},
			TierPolicy: TierPolicyConfig{
				HotRetention:  "1h",
				WarmRetention: "24h",
				ColdRetention: "168h",
			},
			Warm: WarmConfig{
				Enabled:            false,
				BlockSize:          64 * 1024,
				BloomFPRate:        0.01,
				CompactionStrategy: "size_tiered",
				CompactionInterval: "5m",
				MemTableCapacity:   4 * 1024 * 1024,
			},
			Cold: ColdConfig{
				Enabled:              false,
				ArchiveSize:          1024 * 1024 * 1024,
				Compression:          "zstd",
				CompressionLevel:     3,
				DictTrainingInterval: 100,
			},
			Encryption: EncryptionConfig{
				Enabled:   false,
				Algorithm: "aes-256-gcm",
			},
		},
		Defaults: DefaultsConfig{
			Topic: TopicDefaults{
				Partitions:    8,
				RetentionTime: "168h",
				Mode:          "unified",
				Replication:   1,
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
		Limits: LimitsConfig{
			MaxPartitionsPerTopic: 256,
			MaxTopics:             1000,
			MaxFetchMessages:      10000,
			MaxMessageSize:        16 * 1024 * 1024,
			MaxConnections:        10000,
		},
		Protocols: ProtocolsConfig{
			Chimera: ProtocolChimeraConfig{
				Enabled:      true,
				MaxFrameSize: 16 * 1024 * 1024,
			},
			MQTT: ProtocolMQTTConfig{
				Enabled:        false,
				MaxPacketSize:  256 * 1024 * 1024,
				MaxQoS:         2,
				MaxTopicAlias:  16,
				RetainedMax:    10000,
				TopicSeparator: ".",
			},
			WebSocket: ProtocolWebSocketConfig{
				Enabled:      false,
				Path:         "/ws",
				MaxFrameSize: 16 * 1024 * 1024,
			},
			AMQP: ProtocolAMQPConfig{
				Enabled:      false,
				MaxFrameSize: 128 * 1024,
			},
		},
		Cluster: ClusterConfig{
			Enabled: false,
			Raft: RaftConfig{
				ElectionTimeout:   "1s",
				HeartbeatInterval: "150ms",
				SnapshotInterval:  "5m",
				MaxLogEntries:     100000,
			},
			Gossip: GossipConfig{
				BindPort:         5674,
				ProbeInterval:    "1s",
				ProbeTimeout:     "500ms",
				IndirectNodes:    3,
				SuspicionTimeout: "5s",
			},
			Replication: ReplicationConfig{
				DefaultFactor: 3,
				MinISR:        2,
				AckPolicy:     "quorum",
				SyncMode:      "sync",
				MaxLag:        10000,
			},
		},
		Schema: SchemaConfig{
			Enabled:       false,
			DefaultCompat: "backward",
		},
		Priority: PriorityConfig{
			Enabled: false,
			Levels:  10,
		},
		TTL: TTLConfigRoot{
			Enabled:       false,
			ScanInterval:  "1s",
			DefaultAction: "drop",
		},
		WASM: WASMConfig{
			Enabled:          false,
			MaxMemoryPages:   256,
			ExecutionTimeout: "100ms",
			ModulePoolSize:   4,
		},
		Processing: ProcessingConfig{
			Enabled:            false,
			CheckpointInterval: "30s",
		},
		ACL: ACL{
			Enabled:       false,
			DefaultPolicy: "allow",
		},
		GeoReplication: GeoReplicationConfig{
			Enabled:         false,
			ReplicationMode: "async",
			BatchSize:       100,
			FlushInterval:   "1s",
			MaxLag:          "30s",
		},
		FIPS: FIPSConfig{
			Enabled:       false,
			StrictMode:    false,
			MinTLSVersion: "1.2",
		},
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("CHIMERA_NODE_ID"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.Node.ID = n
		}
	}
	if v := os.Getenv("CHIMERA_NODE_NAME"); v != "" {
		cfg.Node.Name = v
	}
	if v := os.Getenv("CHIMERA_DATA_DIR"); v != "" {
		cfg.Node.DataDir = v
	}
	if v := os.Getenv("CHIMERA_LISTEN_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Listener.Port = n
		}
	}
	if v := os.Getenv("CHIMERA_ADMIN_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Listener.AdminPort = n
		}
	}
	if v := os.Getenv("CHIMERA_LOG_LEVEL"); v != "" {
		cfg.Logging.Level = v
	}
	if v := os.Getenv("CHIMERA_LOG_FORMAT"); v != "" {
		cfg.Logging.Format = v
	}
	if v := os.Getenv("CHIMERA_AUTH_ENABLED"); v == "true" {
		cfg.Auth.Enabled = true
	}
	if v := os.Getenv("CHIMERA_TLS_ENABLED"); v == "true" {
		cfg.TLS.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PROTOCOL_MQTT_ENABLED"); v == "true" {
		cfg.Protocols.MQTT.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PROTOCOL_WEBSOCKET_ENABLED"); v == "true" {
		cfg.Protocols.WebSocket.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PROTOCOL_AMQP_ENABLED"); v == "true" {
		cfg.Protocols.AMQP.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PROTOCOL_CHIMERA_ENABLED"); v == "false" {
		cfg.Protocols.Chimera.Enabled = false
	}
	if v := os.Getenv("CHIMERA_CLUSTER_ENABLED"); v == "true" {
		cfg.Cluster.Enabled = true
	}
	if v := os.Getenv("CHIMERA_CLUSTER_GOSSIP_BIND_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Cluster.Gossip.BindPort = n
		}
	}
	if v := os.Getenv("CHIMERA_SCHEMA_ENABLED"); v == "true" {
		cfg.Schema.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PRIORITY_ENABLED"); v == "true" {
		cfg.Priority.Enabled = true
	}
	if v := os.Getenv("CHIMERA_TTL_ENABLED"); v == "true" {
		cfg.TTL.Enabled = true
	}
	if v := os.Getenv("CHIMERA_CLUSTER_REPLICATION_FACTOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Cluster.Replication.DefaultFactor = n
		}
	}
	if v := os.Getenv("CHIMERA_WASM_ENABLED"); v == "true" {
		cfg.WASM.Enabled = true
	}
	if v := os.Getenv("CHIMERA_PROCESSING_ENABLED"); v == "true" {
		cfg.Processing.Enabled = true
	}
}

func applyCLIOverrides(cfg *Config, flags *CLIFlags) {
	if flags.DataDir != "" {
		cfg.Node.DataDir = flags.DataDir
	}
	if flags.Bind != "" {
		cfg.Listener.Bind = flags.Bind
	}
	if flags.Port != 0 {
		cfg.Listener.Port = flags.Port
	}
	if flags.AdminPort != 0 {
		cfg.Listener.AdminPort = flags.AdminPort
	}
	if flags.LogLevel != "" {
		cfg.Logging.Level = flags.LogLevel
	}
}

// Validate checks configuration for errors.
func (c *Config) Validate() error {
	if c.Listener.Port <= 0 || c.Listener.Port > 65535 {
		return fmt.Errorf("listener.port must be 1-65535, got %d", c.Listener.Port)
	}
	if c.Listener.AdminPort <= 0 || c.Listener.AdminPort > 65535 {
		return fmt.Errorf("listener.admin_port must be 1-65535, got %d", c.Listener.AdminPort)
	}
	if c.Node.DataDir == "" {
		return fmt.Errorf("node.data_dir is required")
	}
	if c.Storage.Hot.SyncMode != "immediate" && c.Storage.Hot.SyncMode != "interval" && c.Storage.Hot.SyncMode != "os" {
		return fmt.Errorf("storage.hot.sync_mode must be immediate, interval, or os")
	}
	if c.Storage.WAL.SyncMode != "immediate" && c.Storage.WAL.SyncMode != "interval" && c.Storage.WAL.SyncMode != "os" {
		return fmt.Errorf("storage.wal.sync_mode must be immediate, interval, or os")
	}
	if c.Auth.Enabled {
		switch c.Auth.Type {
		case "static", "file", "oauth", "ldap", "mtls", "scram":
		default:
			return fmt.Errorf("auth.type must be 'static', 'file', 'oauth', or 'ldap'")
		}
		if c.Auth.Type == "static" && len(c.Auth.Users) == 0 && len(c.Auth.Tokens) == 0 {
			return fmt.Errorf("auth.enabled but no users or tokens configured")
		}
	}
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
			return fmt.Errorf("tls.enabled requires cert_file and key_file")
		}
	}
	if c.Limits.MaxPartitionsPerTopic == 0 {
		return fmt.Errorf("limits.max_partitions_per_topic must be > 0")
	}
	if c.Limits.MaxTopics <= 0 {
		return fmt.Errorf("limits.max_topics must be > 0")
	}
	if c.Limits.MaxFetchMessages <= 0 {
		return fmt.Errorf("limits.max_fetch_messages must be > 0")
	}
	if c.Cluster.Enabled {
		if len(c.Cluster.Raft.Peers) == 0 {
			return fmt.Errorf("cluster.raft.peers is required when cluster is enabled")
		}
		if c.Cluster.Replication.DefaultFactor < 1 {
			return fmt.Errorf("cluster.replication.default_factor must be >= 1")
		}
		if c.Cluster.Replication.MinISR < 1 {
			return fmt.Errorf("cluster.replication.min_isr must be >= 1")
		}
		if c.Cluster.Replication.MinISR > c.Cluster.Replication.DefaultFactor {
			return fmt.Errorf("cluster.replication.min_isr cannot exceed default_factor")
		}
		switch c.Cluster.Replication.AckPolicy {
		case "leader", "quorum", "all":
		default:
			return fmt.Errorf("cluster.replication.ack_policy must be leader, quorum, or all")
		}
		switch c.Cluster.Replication.SyncMode {
		case "sync", "async":
		default:
			return fmt.Errorf("cluster.replication.sync_mode must be sync or async")
		}
		if _, err := time.ParseDuration(c.Cluster.Raft.ElectionTimeout); err != nil {
			return fmt.Errorf("cluster.raft.election_timeout is invalid: %w", err)
		}
		if _, err := time.ParseDuration(c.Cluster.Raft.HeartbeatInterval); err != nil {
			return fmt.Errorf("cluster.raft.heartbeat_interval is invalid: %w", err)
		}
	}
	if c.FIPS.Enabled {
		if c.FIPS.MinTLSVersion != "" && c.FIPS.MinTLSVersion != "1.2" && c.FIPS.MinTLSVersion != "1.3" {
			return fmt.Errorf("fips.min_tls_version must be '1.2' or '1.3'")
		}
		// FIPS requires TLS when enabled
		if !c.TLS.Enabled {
			return fmt.Errorf("fips.enabled requires tls.enabled")
		}
	}
	return nil
}

// ParseDuration parses a duration string, returning default if empty.
func ParseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}
