package broker

import (
	"os"
	"testing"
)

// --- Validate coverage ---

func TestValidate_InvalidPortTooHigh(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for port > 65535")
	}
}

func TestValidate_InvalidAdminPortNegative(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.AdminPort = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative admin port")
	}
}

func TestValidate_AdminPortZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.AdminPort = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for admin port 0")
	}
}

func TestValidate_WALSyncModeOS(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.WAL.SyncMode = "os"
	if err := cfg.Validate(); err != nil {
		t.Errorf("os should be valid WAL sync mode: %v", err)
	}
}

func TestValidate_WALSyncModeImmediate(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.WAL.SyncMode = "immediate"
	if err := cfg.Validate(); err != nil {
		t.Errorf("immediate should be valid WAL sync mode: %v", err)
	}
}

func TestValidate_HotSyncModeOS(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Hot.SyncMode = "os"
	if err := cfg.Validate(); err != nil {
		t.Errorf("os should be valid hot sync mode: %v", err)
	}
}

func TestValidate_HotSyncModeImmediate(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Hot.SyncMode = "immediate"
	if err := cfg.Validate(); err != nil {
		t.Errorf("immediate should be valid hot sync mode: %v", err)
	}
}

func TestValidate_AuthEnabledInvalidType(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid auth type")
	}
}

func TestValidate_AuthStaticNoUsersOrTokens(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for static auth with no users or tokens")
	}
}

func TestValidate_AuthStaticWithUsers(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Users = map[string]string{"admin": "pass"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("static auth with users should be valid: %v", err)
	}
}

func TestValidate_AuthStaticWithTokens(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Tokens = map[string]string{"token1": "user1"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("static auth with tokens should be valid: %v", err)
	}
}

func TestValidate_AuthTypeFile(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "file"
	cfg.Auth.AuthFile = "/tmp/passwd"
	if err := cfg.Validate(); err != nil {
		t.Errorf("file auth should be valid: %v", err)
	}
}

func TestValidate_AuthTypeOAuth(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "oauth"
	cfg.Auth.OAuth = OAuthConfig{Issuer: "https://issuer.example.com"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("oauth auth should be valid: %v", err)
	}
}

func TestValidate_AuthTypeLDAP(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "ldap"
	cfg.Auth.LDAP = LDAPConfig{URL: "ldap://localhost"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("ldap auth should be valid: %v", err)
	}
}

func TestValidate_AuthTypeMTLS(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "mtls"
	if err := cfg.Validate(); err != nil {
		t.Errorf("mtls auth should be valid: %v", err)
	}
}

func TestValidate_TLSEnabledNoCert(t *testing.T) {
	cfg := defaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = ""
	cfg.TLS.KeyFile = "key.pem"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for TLS enabled without cert_file")
	}
}

func TestValidate_TLSEnabledNoKey(t *testing.T) {
	cfg := defaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "cert.pem"
	cfg.TLS.KeyFile = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for TLS enabled without key_file")
	}
}

func TestValidate_TLSEnabledWithCertAndKey(t *testing.T) {
	cfg := defaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "cert.pem"
	cfg.TLS.KeyFile = "key.pem"
	if err := cfg.Validate(); err != nil {
		t.Errorf("TLS with cert and key should be valid: %v", err)
	}
}

func TestValidate_MaxPartitionsZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Limits.MaxPartitionsPerTopic = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_partitions_per_topic = 0")
	}
}

func TestValidate_MaxTopicsZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Limits.MaxTopics = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_topics = 0")
	}
}

func TestValidate_MaxTopicsNegative(t *testing.T) {
	cfg := defaultConfig()
	cfg.Limits.MaxTopics = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_topics < 0")
	}
}

func TestValidate_MaxFetchMessagesZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Limits.MaxFetchMessages = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_fetch_messages = 0")
	}
}

func TestValidate_MaxFetchMessagesNegative(t *testing.T) {
	cfg := defaultConfig()
	cfg.Limits.MaxFetchMessages = -5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for max_fetch_messages < 0")
	}
}

func TestValidate_ClusterEnabledNoPeers(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = nil
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for cluster enabled without raft peers")
	}
}

func TestValidate_ClusterEnabledNoHMACKey(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Gossip.HMACKey = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for cluster enabled without hmac_key")
	}
}

func TestValidate_ClusterEnabledReplicationFactorZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Replication.DefaultFactor = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for replication factor < 1")
	}
}

func TestValidate_ClusterMinISRZero(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Replication.DefaultFactor = 3
	cfg.Cluster.Replication.MinISR = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for min_isr < 1")
	}
}

func TestValidate_ClusterMinISRExceedsFactor(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Replication.DefaultFactor = 2
	cfg.Cluster.Replication.MinISR = 3
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for min_isr > default_factor")
	}
}

func TestValidate_ClusterInvalidAckPolicy(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Replication.AckPolicy = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid ack_policy")
	}
}

func TestValidate_ClusterValidAckPolicies(t *testing.T) {
	for _, policy := range []string{"leader", "quorum", "all"} {
		cfg := defaultConfig()
		cfg.Cluster.Enabled = true
		cfg.Cluster.Raft.Peers = []string{"node1:5672"}
		cfg.Cluster.Gossip.HMACKey = "test-secret"
		cfg.Cluster.Replication.AckPolicy = policy
		if err := cfg.Validate(); err != nil {
			t.Errorf("ack_policy=%q should be valid: %v", policy, err)
		}
	}
}

func TestValidate_ClusterInvalidSyncMode(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Replication.SyncMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid sync_mode")
	}
}

func TestValidate_ClusterValidSyncModes(t *testing.T) {
	for _, mode := range []string{"sync", "async"} {
		cfg := defaultConfig()
		cfg.Cluster.Enabled = true
		cfg.Cluster.Raft.Peers = []string{"node1:5672"}
		cfg.Cluster.Gossip.HMACKey = "test-secret"
		cfg.Cluster.Replication.SyncMode = mode
		if err := cfg.Validate(); err != nil {
			t.Errorf("sync_mode=%q should be valid: %v", mode, err)
		}
	}
}

func TestValidate_ClusterInvalidElectionTimeout(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Raft.ElectionTimeout = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid election_timeout")
	}
}

func TestValidate_ClusterInvalidHeartbeatInterval(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Raft.HeartbeatInterval = "bad"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid heartbeat_interval")
	}
}

func TestValidate_ClusterValid(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672", "node2:5672"}
	cfg.Cluster.Gossip.HMACKey = "test-secret"
	cfg.Cluster.Replication.DefaultFactor = 3
	cfg.Cluster.Replication.MinISR = 2
	cfg.Cluster.Replication.AckPolicy = "quorum"
	cfg.Cluster.Replication.SyncMode = "sync"
	cfg.Cluster.Raft.ElectionTimeout = "1s"
	cfg.Cluster.Raft.HeartbeatInterval = "150ms"
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid cluster config should pass: %v", err)
	}
}

// --- applyEnvOverrides coverage ---

func TestApplyEnvOverrides_NodeIDInvalid(t *testing.T) {
	os.Setenv("CHIMERA_NODE_ID", "not-a-number")
	defer os.Unsetenv("CHIMERA_NODE_ID")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	// Invalid number should not change the default
	if cfg.Node.ID != 1 {
		t.Errorf("invalid CHIMERA_NODE_ID should not change default, got %d", cfg.Node.ID)
	}
}

func TestApplyEnvOverrides_NodeName(t *testing.T) {
	os.Setenv("CHIMERA_NODE_NAME", "custom-node")
	defer os.Unsetenv("CHIMERA_NODE_NAME")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Node.Name != "custom-node" {
		t.Errorf("expected custom-node, got %q", cfg.Node.Name)
	}
}

func TestApplyEnvOverrides_DataDir(t *testing.T) {
	os.Setenv("CHIMERA_DATA_DIR", "/data/custom")
	defer os.Unsetenv("CHIMERA_DATA_DIR")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Node.DataDir != "/data/custom" {
		t.Errorf("expected /data/custom, got %q", cfg.Node.DataDir)
	}
}

func TestApplyEnvOverrides_ListenPortInvalid(t *testing.T) {
	os.Setenv("CHIMERA_LISTEN_PORT", "abc")
	defer os.Unsetenv("CHIMERA_LISTEN_PORT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	// Invalid port should not change the default
	if cfg.Listener.Port != 5672 {
		t.Errorf("invalid CHIMERA_LISTEN_PORT should not change default, got %d", cfg.Listener.Port)
	}
}

func TestApplyEnvOverrides_AdminPort(t *testing.T) {
	os.Setenv("CHIMERA_ADMIN_PORT", "8080")
	defer os.Unsetenv("CHIMERA_ADMIN_PORT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Listener.AdminPort != 8080 {
		t.Errorf("expected 8080, got %d", cfg.Listener.AdminPort)
	}
}

func TestApplyEnvOverrides_AdminPortInvalid(t *testing.T) {
	os.Setenv("CHIMERA_ADMIN_PORT", "xyz")
	defer os.Unsetenv("CHIMERA_ADMIN_PORT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Listener.AdminPort != 9090 {
		t.Errorf("invalid CHIMERA_ADMIN_PORT should not change default, got %d", cfg.Listener.AdminPort)
	}
}

func TestApplyEnvOverrides_LogLevel(t *testing.T) {
	os.Setenv("CHIMERA_LOG_LEVEL", "debug")
	defer os.Unsetenv("CHIMERA_LOG_LEVEL")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Logging.Level != "debug" {
		t.Errorf("expected debug, got %q", cfg.Logging.Level)
	}
}

func TestApplyEnvOverrides_LogFormat(t *testing.T) {
	os.Setenv("CHIMERA_LOG_FORMAT", "text")
	defer os.Unsetenv("CHIMERA_LOG_FORMAT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Logging.Format != "text" {
		t.Errorf("expected text, got %q", cfg.Logging.Format)
	}
}

func TestApplyEnvOverrides_AuthEnabled(t *testing.T) {
	os.Setenv("CHIMERA_AUTH_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_AUTH_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Auth.Enabled {
		t.Error("expected Auth.Enabled = true")
	}
}

func TestApplyEnvOverrides_TLSEnabled(t *testing.T) {
	os.Setenv("CHIMERA_TLS_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_TLS_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.TLS.Enabled {
		t.Error("expected TLS.Enabled = true")
	}
}

func TestApplyEnvOverrides_MQTTEnabled(t *testing.T) {
	os.Setenv("CHIMERA_PROTOCOL_MQTT_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_PROTOCOL_MQTT_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Protocols.MQTT.Enabled {
		t.Error("expected MQTT.Enabled = true")
	}
}

func TestApplyEnvOverrides_WebSocketEnabled(t *testing.T) {
	os.Setenv("CHIMERA_PROTOCOL_WEBSOCKET_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_PROTOCOL_WEBSOCKET_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Protocols.WebSocket.Enabled {
		t.Error("expected WebSocket.Enabled = true")
	}
}

func TestApplyEnvOverrides_AMQPEnabled(t *testing.T) {
	os.Setenv("CHIMERA_PROTOCOL_AMQP_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_PROTOCOL_AMQP_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Protocols.AMQP.Enabled {
		t.Error("expected AMQP.Enabled = true")
	}
}

func TestApplyEnvOverrides_ChimeraDisabled(t *testing.T) {
	os.Setenv("CHIMERA_PROTOCOL_CHIMERA_ENABLED", "false")
	defer os.Unsetenv("CHIMERA_PROTOCOL_CHIMERA_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Protocols.Chimera.Enabled {
		t.Error("expected Chimera.Enabled = false")
	}
}

func TestApplyEnvOverrides_ClusterEnabled(t *testing.T) {
	os.Setenv("CHIMERA_CLUSTER_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_CLUSTER_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Cluster.Enabled {
		t.Error("expected Cluster.Enabled = true")
	}
}

func TestApplyEnvOverrides_GossipBindPort(t *testing.T) {
	os.Setenv("CHIMERA_CLUSTER_GOSSIP_BIND_PORT", "9999")
	defer os.Unsetenv("CHIMERA_CLUSTER_GOSSIP_BIND_PORT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Cluster.Gossip.BindPort != 9999 {
		t.Errorf("expected 9999, got %d", cfg.Cluster.Gossip.BindPort)
	}
}

func TestApplyEnvOverrides_GossipBindPortInvalid(t *testing.T) {
	os.Setenv("CHIMERA_CLUSTER_GOSSIP_BIND_PORT", "bad")
	defer os.Unsetenv("CHIMERA_CLUSTER_GOSSIP_BIND_PORT")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Cluster.Gossip.BindPort != 5674 {
		t.Errorf("invalid port should not change default, got %d", cfg.Cluster.Gossip.BindPort)
	}
}

func TestApplyEnvOverrides_SchemaEnabled(t *testing.T) {
	os.Setenv("CHIMERA_SCHEMA_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_SCHEMA_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Schema.Enabled {
		t.Error("expected Schema.Enabled = true")
	}
}

func TestApplyEnvOverrides_PriorityEnabled(t *testing.T) {
	os.Setenv("CHIMERA_PRIORITY_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_PRIORITY_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Priority.Enabled {
		t.Error("expected Priority.Enabled = true")
	}
}

func TestApplyEnvOverrides_TTLEnabled(t *testing.T) {
	os.Setenv("CHIMERA_TTL_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_TTL_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.TTL.Enabled {
		t.Error("expected TTL.Enabled = true")
	}
}

func TestApplyEnvOverrides_ReplicationFactor(t *testing.T) {
	os.Setenv("CHIMERA_CLUSTER_REPLICATION_FACTOR", "5")
	defer os.Unsetenv("CHIMERA_CLUSTER_REPLICATION_FACTOR")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Cluster.Replication.DefaultFactor != 5 {
		t.Errorf("expected 5, got %d", cfg.Cluster.Replication.DefaultFactor)
	}
}

func TestApplyEnvOverrides_ReplicationFactorInvalid(t *testing.T) {
	os.Setenv("CHIMERA_CLUSTER_REPLICATION_FACTOR", "bad")
	defer os.Unsetenv("CHIMERA_CLUSTER_REPLICATION_FACTOR")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if cfg.Cluster.Replication.DefaultFactor != 3 {
		t.Errorf("invalid factor should not change default, got %d", cfg.Cluster.Replication.DefaultFactor)
	}
}

func TestApplyEnvOverrides_WASMEnabled(t *testing.T) {
	os.Setenv("CHIMERA_WASM_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_WASM_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.WASM.Enabled {
		t.Error("expected WASM.Enabled = true")
	}
}

func TestApplyEnvOverrides_ProcessingEnabled(t *testing.T) {
	os.Setenv("CHIMERA_PROCESSING_ENABLED", "true")
	defer os.Unsetenv("CHIMERA_PROCESSING_ENABLED")

	cfg := defaultConfig()
	applyEnvOverrides(cfg)
	if !cfg.Processing.Enabled {
		t.Error("expected Processing.Enabled = true")
	}
}

func TestValidate_GeoReplicationMissingFields(t *testing.T) {
	cfg := defaultConfig()
	cfg.GeoReplication.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when geo enabled but no local_dc")
	}

	cfg.GeoReplication.LocalDC = "us-east"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when geo enabled but no remote DCs")
	}

	cfg.GeoReplication.RemoteDCs = []GeoRemoteDCConfig{{ID: "us-west"}}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when remote DC has no address")
	}

	cfg.GeoReplication.RemoteDCs = []GeoRemoteDCConfig{{ID: "us-west", Address: "http://remote:8080"}}
	cfg.GeoReplication.ReplicationMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid replication_mode")
	}
}

func TestValidate_GeoReplicationValid(t *testing.T) {
	cfg := defaultConfig()
	cfg.GeoReplication.Enabled = true
	cfg.GeoReplication.LocalDC = "us-east"
	cfg.GeoReplication.RemoteDCs = []GeoRemoteDCConfig{{ID: "us-west", Address: "http://remote:8080"}}
	cfg.GeoReplication.ReplicationMode = "async"
	if err := cfg.Validate(); err != nil {
		t.Errorf("unexpected error for valid geo config: %v", err)
	}
}

func TestValidate_ClusterInvalidDurations(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Gossip.HMACKey = "test-secret"
	cfg.Cluster.Raft.ElectionTimeout = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid election_timeout")
	}

	cfg.Cluster.Raft.ElectionTimeout = "200ms"
	cfg.Cluster.Raft.HeartbeatInterval = "bad"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid heartbeat_interval")
	}
}

func TestValidate_ClusterValidDurations(t *testing.T) {
	cfg := defaultConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Raft.Peers = []string{"node1:5672"}
	cfg.Cluster.Gossip.HMACKey = "test-secret"
	cfg.Cluster.Raft.ElectionTimeout = "200ms"
	cfg.Cluster.Raft.HeartbeatInterval = "50ms"
	if err := cfg.Validate(); err != nil {
		t.Errorf("unexpected error for valid cluster config: %v", err)
	}
}

func TestValidate_DataDirRequired(t *testing.T) {
	cfg := defaultConfig()
	cfg.Node.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty data_dir")
	}
}

func TestValidate_InvalidHotSyncMode(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Hot.SyncMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid hot sync_mode")
	}
}
