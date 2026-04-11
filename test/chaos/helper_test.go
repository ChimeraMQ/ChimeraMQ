package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	adminhttp "github.com/chimeramq/chimera/internal/protocol/http"
)

type testBroker struct {
	broker *broker.Broker
	server *adminhttp.AdminServer
	addr   string
	tmpDir string
}

func newTestBroker(t *testing.T) *testBroker {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "chimera-chaos-*")
	if err != nil {
		t.Fatal(err)
	}

	port := 39000 + rand.Intn(1000)
	adminPort := port + 1000

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "chaos-test",
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
				SegmentSize:  1024 * 1024,
				SyncMode:     "immediate",
				SyncInterval: "50ms",
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
			Level:  "error",
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
	go func() { _ = srv.Serve() }()

	addr := fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(addr + "/v1/health")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tb := &testBroker{
		broker: b,
		server: srv,
		addr:   addr,
		tmpDir: tmpDir,
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		tb.server.Shutdown(ctx)
		cancel()
		tb.broker.Stop()
		os.RemoveAll(tb.tmpDir)
	})

	return tb
}
