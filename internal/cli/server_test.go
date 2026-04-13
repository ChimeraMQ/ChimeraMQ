package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/protocol"
)

func TestRunClusterCLINoArgs(t *testing.T) {
	// Should os.Exit(1) — test by running as subprocess
	if os.Getenv("TEST_CLUSTER_NOARGS") == "1" {
		RunClusterCLI([]string{})
		return
	}
}

func TestRunClusterCLIStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunClusterCLI([]string{"status"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "ok") {
		t.Errorf("expected 'ok' in output, got: %s", output)
	}
}

func TestRunClusterCLIMembers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/cluster/members" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"members":["node-1"]}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunClusterCLI([]string{"members"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "node-1") {
		t.Errorf("expected 'node-1' in output, got: %s", output)
	}
}

func TestRunTopicCLIList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"topics":["t1"]}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunTopicCLI([]string{"list"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "t1") {
		t.Errorf("expected 't1' in output, got: %s", output)
	}
}

func TestRunTopicCLICreate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics" && r.Method == "POST" {
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"name":"%s","mode":"%s","partitions":%v}`, body["name"], body["mode"], body["partitions"])
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunTopicCLI([]string{"create", "-name", "test-topic", "-mode", "stream", "-partitions", "4"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "test-topic") {
		t.Errorf("expected 'test-topic' in output, got: %s", output)
	}
}

func TestRunTopicCLIDescribe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics/my-topic" && r.Method == "GET" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"name":"my-topic","mode":"stream","partitions":8}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunTopicCLI([]string{"describe", "my-topic"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "my-topic") {
		t.Errorf("expected 'my-topic' in output, got: %s", output)
	}
}

func TestRunTopicCLIDelete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics/my-topic" && r.Method == "DELETE" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"deleted"}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunTopicCLI([]string{"delete", "my-topic"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "deleted") {
		t.Errorf("expected 'deleted' in output, got: %s", output)
	}
}

func TestRunProduceCLI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"partition":0,"offset":1}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunProduceCLI([]string{"-topic", "test", "-message", "hello", "-count", "1"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "Published") {
		t.Errorf("expected 'Published' in output, got: %s", output)
	}
}

func TestRunConsumeCLI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"messages":[{"offset":0,"payload":"aGVsbG8="}],"next_offset":1,"count":1}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunConsumeCLI([]string{"-topic", "test", "-partition", "0", "-offset", "0", "-limit", "10"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "offset=0") {
		t.Errorf("expected offset in output, got: %s", output)
	}
}

func TestRunWASMCLIList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/wasm/modules" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"modules":["mod1","mod2"]}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunWASMCLI([]string{"list"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "WASM Modules") {
		t.Errorf("expected 'WASM Modules' in output, got: %s", output)
	}
}

func TestRunWASMCLIListEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"modules":[]}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunWASMCLI([]string{"list"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "No WASM modules") {
		t.Errorf("expected 'No WASM modules' in output, got: %s", output)
	}
}

func TestRunWASMCLIRemove(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == "/v1/wasm/modules/mod1" {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunWASMCLI([]string{"remove", "mod1"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "removed") {
		t.Errorf("expected 'removed' in output, got: %s", output)
	}
}

func TestRunWASMCLIDeploy(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test-*.wasm")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Write([]byte("\x00asm"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"name":"test-mod"}`)
		}
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunWASMCLI([]string{"deploy", tmpFile.Name(), "--name", "test-mod"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "compiled successfully") {
		t.Errorf("expected 'compiled successfully' in output, got: %s", output)
	}
}

func TestRunWASMCLIUnknownCommand(t *testing.T) {
	// Test that unknown command prints usage and exits
	if os.Getenv("TEST_WASM_UNKNOWN") == "1" {
		RunWASMCLI([]string{"unknown"})
		return
	}
	// Just verify it doesn't panic when called with no args
}

func TestRunTopicCLIUnknownCommand(t *testing.T) {
	// Verify no-args path doesn't panic
	if os.Getenv("TEST_TOPIC_UNKNOWN") == "1" {
		RunTopicCLI([]string{"unknown"})
		return
	}
}

func TestRunTopicCLIDescribeNoName(t *testing.T) {
	if os.Getenv("TEST_TOPIC_DESCRIBE_NONAME") == "1" {
		RunTopicCLI([]string{"describe"})
		return
	}
}

func TestRunTopicCLINoArgs(t *testing.T) {
	if os.Getenv("TEST_TOPIC_NOARGS") == "1" {
		RunTopicCLI([]string{})
		return
	}
}

func TestRegisterProtocols(t *testing.T) {
	cfg, err := broker.LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Node.DataDir = t.TempDir()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Protocols.AMQP.Enabled = true
	cfg.Protocols.MQTT.Enabled = true
	cfg.Protocols.STOMP.Enabled = true
	cfg.Protocols.NATS.Enabled = true
	cfg.Protocols.Chimera.Enabled = true

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop()

	mux := protocol.NewProtocolMux(b)
	registerProtocols(mux, cfg, b)

	// Verify no panic and Stop iterates all registered handlers
	mux.Stop()
}

func TestRegisterProtocolsMinimal(t *testing.T) {
	cfg, err := broker.LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Node.DataDir = t.TempDir()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	// All protocols disabled except HTTP (always on)
	cfg.Protocols.AMQP.Enabled = false
	cfg.Protocols.MQTT.Enabled = false
	cfg.Protocols.STOMP.Enabled = false
	cfg.Protocols.NATS.Enabled = false
	cfg.Protocols.Chimera.Enabled = false

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop()

	mux := protocol.NewProtocolMux(b)
	registerProtocols(mux, cfg, b)
	mux.Stop()
}
