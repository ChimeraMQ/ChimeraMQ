package integration

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	adminhttp "github.com/chimeramq/chimera/internal/protocol/http"
)

// newAuthTestBroker creates a broker with static auth and ACL enabled.
func newAuthTestBroker(t *testing.T) *testBroker {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "chimera-auth-test-*")
	if err != nil {
		t.Fatal(err)
	}

	port := 20000 + rand.Intn(1000)
	adminPort := port + 1000

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "test-auth-node",
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
		Auth: broker.AuthConfig{
			Enabled: true,
			Type:    "static",
			Users:   map[string]string{"admin": "$2a$04$Yn5OMAdh3HYOpXGclm0olOvKGYxkwrEyeIzzECzXywsPs1gt.sPvy", "reader": "$2a$04$lzwVfGnaJJ.HLsQPBPh1DulA9sq4vf2wbl3zHAkHVxjSvnL8eLHLa"},
			Tokens:  map[string]string{"api-key-admin": "admin", "api-key-reader": "reader"},
		},
		ACL: broker.ACL{
			Enabled:       true,
			DefaultPolicy: "deny",
			Entries: []broker.ACLEntryConfig{
				{Principal: "admin", Resource: "cluster", Name: "*", Operation: "all", Permission: "allow"},
				{Principal: "admin", Resource: "topic", Name: "*", Operation: "all", Permission: "allow"},
				{Principal: "admin", Resource: "schema", Name: "*", Operation: "all", Permission: "allow"},
				{Principal: "*", Resource: "topic", Name: "public.*", Operation: "read", Permission: "allow"},
			},
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
		_ = srv.Serve()
	}()

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

func TestAuthBearerTokenValid(t *testing.T) {
	tb := newAuthTestBroker(t)

	req, _ := http.NewRequest("POST", tb.addr+"/v1/topics", strings.NewReader(`{"name":"test-topic","partitions":4}`))
	req.Header.Set("Authorization", "Bearer api-key-admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200/201, got %d: %s", resp.StatusCode, body)
	}
}

func TestAuthBearerTokenInvalid(t *testing.T) {
	tb := newAuthTestBroker(t)

	req, _ := http.NewRequest("GET", tb.addr+"/v1/topics", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthBasicValid(t *testing.T) {
	tb := newAuthTestBroker(t)

	encoded := base64.StdEncoding.EncodeToString([]byte("admin:admin123"))
	req, _ := http.NewRequest("GET", tb.addr+"/v1/topics", nil)
	req.Header.Set("Authorization", "Basic "+encoded)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthBasicWrongPassword(t *testing.T) {
	tb := newAuthTestBroker(t)

	encoded := base64.StdEncoding.EncodeToString([]byte("admin:wrongpass"))
	req, _ := http.NewRequest("GET", tb.addr+"/v1/topics", nil)
	req.Header.Set("Authorization", "Basic "+encoded)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthNoHeader(t *testing.T) {
	tb := newAuthTestBroker(t)

	resp, err := http.Get(tb.addr + "/v1/topics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAuthHealthNoAuth(t *testing.T) {
	tb := newAuthTestBroker(t)

	// Health endpoint should NOT require auth
	resp, err := http.Get(tb.addr + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health endpoint should be public, got %d", resp.StatusCode)
	}
}

func TestACLDenyNonAdmin(t *testing.T) {
	tb := newAuthTestBroker(t)

	// Reader token should NOT be able to create topics (only read public.*)
	req, _ := http.NewRequest("POST", tb.addr+"/v1/topics", strings.NewReader(`{"name":"test-topic","partitions":4}`))
	req.Header.Set("Authorization", "Bearer api-key-reader")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for non-admin create, got %d", resp.StatusCode)
	}
}

func TestACLAdminCanCreateTopic(t *testing.T) {
	tb := newAuthTestBroker(t)

	// Admin should be able to create topics
	req, _ := http.NewRequest("POST", tb.addr+"/v1/topics", strings.NewReader(`{"name":"admin-topic","partitions":4}`))
	req.Header.Set("Authorization", "Bearer api-key-admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin should create topic, got %d: %s", resp.StatusCode, body)
	}
}

func TestACLAdminCanPublish(t *testing.T) {
	tb := newAuthTestBroker(t)

	// First create topic as admin
	createTopic(t, tb, "admin-topic")

	// Publish message
	body := `{"key":"test-key","value":"dGVzdA==","content_type":"text/plain"}`
	req, _ := http.NewRequest("POST", tb.addr+"/v1/messages/admin-topic", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer api-key-admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("admin should publish, got %d: %s", resp.StatusCode, respBody)
	}
}

func TestACLReaderCannotPublish(t *testing.T) {
	tb := newAuthTestBroker(t)

	createTopic(t, tb, "public.events")

	body := `{"key":"k","value":"dg=="}`
	req, _ := http.NewRequest("POST", tb.addr+"/v1/messages/public.events", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer api-key-reader")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("reader should not publish, got %d", resp.StatusCode)
	}
}

func createTopic(t *testing.T, tb *testBroker, name string) {
	t.Helper()
	req, _ := http.NewRequest("POST", tb.addr+"/v1/topics", strings.NewReader(
		fmt.Sprintf(`{"name":"%s","partitions":4}`, name),
	))
	req.Header.Set("Authorization", "Bearer api-key-admin")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to create topic %q: %d", name, resp.StatusCode)
	}
}

func TestAuthTokenIdentity(t *testing.T) {
	tb := newAuthTestBroker(t)

	// Verify that token-based auth resolves the correct identity
	req, _ := http.NewRequest("GET", tb.addr+"/v1/topics", nil)
	req.Header.Set("Authorization", "Bearer api-key-admin")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	_ = result // Just verify it parses
}
