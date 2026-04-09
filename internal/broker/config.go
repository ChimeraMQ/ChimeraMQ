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
	Node     NodeConfig     `yaml:"node"`
	Listener ListenerConfig `yaml:"listener"`
	Storage  StorageConfig  `yaml:"storage"`
	Defaults DefaultsConfig `yaml:"defaults"`
	Logging  LoggingConfig  `yaml:"logging"`
}

// NodeConfig identifies this broker node.
type NodeConfig struct {
	ID      uint64 `yaml:"id"`
	Name    string `yaml:"name"`
	DataDir string `yaml:"data_dir"`
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
	TierPolicy TierPolicyConfig `yaml:"tier_policy"`
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
	HotRetention string `yaml:"hot_retention"`
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
}

// LoggingConfig controls structured logging.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
	File   string `yaml:"file"`
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
			Bind:           "0.0.0.0",
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
				HotRetention: "1h",
			},
		},
		Defaults: DefaultsConfig{
			Topic: TopicDefaults{
				Partitions:    8,
				RetentionTime: "168h",
				Mode:          "unified",
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
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
