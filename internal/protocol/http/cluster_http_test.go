package http

import (
	"net/http"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

func TestClusterMembersClustered(t *testing.T) {
	dir, err := os.MkdirTemp("", "chimera-http-cluster-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "cluster-node", DataDir: dir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Cluster: broker.ClusterConfig{
			Enabled: true,
			Raft: broker.RaftConfig{
				ElectionTimeout:   "1s",
				HeartbeatInterval: "150ms",
				SnapshotInterval:  "5m",
				MaxLogEntries:     100000,
			},
			Gossip: broker.GossipConfig{
				BindPort:         0,
				ProbeInterval:    "1s",
				ProbeTimeout:     "500ms",
				SuspicionTimeout: "5s",
			},
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop()

	srv := NewAdminServer(b)

	resp := doRequest(t, srv, "GET", "/v1/cluster/members", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
