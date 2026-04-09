package integration

import (
	"context"
	"fmt"
	"math/rand"
	nethttp "net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	adminhttp "github.com/chimeramq/chimera/internal/protocol/http"
)

// testBroker wraps a broker + admin server for integration testing.
type testBroker struct {
	broker *broker.Broker
	server *adminhttp.AdminServer
	addr   string
	tmpDir string
}

func newTestBroker(t *testing.T) *testBroker {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "chimera-test-*")
	if err != nil {
		t.Fatal(err)
	}

	port := 19000 + rand.Intn(1000)
	adminPort := port + 1000

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "test-node",
			DataDir: tmpDir,
		},
		Listener: broker.ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           port,
			AdminPort:      adminPort,
			MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{
				SegmentSize:  1024 * 1024, // 1MB for testing
				SyncMode:     "immediate",
				SyncInterval: "50ms",
				MaxSegments:  5,
			},
			WAL: broker.WALConfig{
				MaxSize:      4 * 1024 * 1024,
				SyncMode:     "immediate",
				SyncInterval: "50ms",
			},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{
				Partitions:    4,
				RetentionTime: "1h",
				Mode:          "unified",
			},
		},
		Logging: broker.LoggingConfig{
			Level:  "warn",
			Format: "text",
			Output: "stdout",
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create broker: %v", err)
	}

	if err := b.Start(); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("start broker: %v", err)
	}

	srv := adminhttp.NewAdminServer(b)

	go func() {
		if err := srv.Serve(); err != nil && err != nethttp.ErrServerClosed {
			// Server stopped
		}
	}()

	// Wait for server to be ready
	addr := fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	waitForServer(t, addr)

	tb := &testBroker{
		broker: b,
		server: srv,
		addr:   addr,
		tmpDir: tmpDir,
	}

	t.Cleanup(tb.close)
	return tb
}

func (tb *testBroker) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tb.server.Shutdown(ctx)
	tb.broker.Stop()
	os.RemoveAll(tb.tmpDir)
}

func (tb *testBroker) dataDir() string {
	return tb.tmpDir
}

// recreateBroker stops the current broker/server and creates a new one
// pointing at the same data directory. Used for crash recovery tests.
func (tb *testBroker) recreateBroker(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	tb.server.Shutdown(ctx)
	cancel()
	tb.broker.Stop()

	cfg := tb.broker.Config()
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("recreate broker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("restart broker: %v", err)
	}

	tb.broker = b
	tb.server = adminhttp.NewAdminServer(b)

	go func() {
		tb.server.Serve()
	}()

	waitForServer(t, tb.addr)
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		resp, err := nethttp.Get(addr + "/v1/health")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not become ready in time")
}

// tmpDataDir creates a temporary data directory for recovery tests.
func tmpDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "chimera-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func topicMetaPath(dataDir, topic string) string {
	return filepath.Join(dataDir, "topics", topic, "meta.json")
}
