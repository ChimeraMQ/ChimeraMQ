package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestMainNoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_NOARGS") == "1" {
		os.Args = []string{"chimera"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainNoArgsSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_NOARGS=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for no args")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestMainUnknownCommandSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_UNKNOWN") == "1" {
		os.Args = []string{"chimera", "unknown-cmd"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainUnknownCommandSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_UNKNOWN=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unknown command")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestMainVersionSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_VERSION") == "1" {
		os.Args = []string{"chimera", "version"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainVersionSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_VERSION=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("version command failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "ChimeraMQ") {
		t.Errorf("expected version output, got: %s", string(output))
	}
}

func TestPrintUsage(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "server") {
		t.Errorf("expected usage to contain 'server', got: %s", output)
	}
}

func TestPrintVersion(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printVersion()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "ChimeraMQ") {
		t.Errorf("expected version output, got: %s", output)
	}
}

func TestMainServerHelpSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_SERVER_HELP") == "1" {
		os.Args = []string{"chimera", "server", "--help"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainServerHelpSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_SERVER_HELP=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("server --help failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "server") {
		t.Errorf("expected help output, got: %s", string(output))
	}
}

func TestMainTopicListSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"topics":["t1"]}`)
		}
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_TOPIC_LIST") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "topic", "list"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainTopicListSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_TOPIC_LIST=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("topic list failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "t1") {
		t.Errorf("expected topic in output, got: %s", string(output))
	}
}

func TestMainClusterStatusSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
		}
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_CLUSTER_STATUS") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "cluster", "status"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainClusterStatusSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_CLUSTER_STATUS=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cluster status failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "ok") {
		t.Errorf("expected status in output, got: %s", string(output))
	}
}

func TestMainWASMListSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/wasm/modules" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"modules":["mod1"]}`)
		}
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_WASM_LIST") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "wasm", "list"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainWASMListSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_WASM_LIST=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("wasm list failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "mod1") {
		t.Errorf("expected module in output, got: %s", string(output))
	}
}

func TestMainProduceSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"partition":0,"offset":1}`)
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_PRODUCE") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "produce", "-topic", "test", "-message", "hello"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainProduceSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_PRODUCE=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("produce failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "Published") {
		t.Errorf("expected published message in output, got: %s", string(output))
	}
}

func TestMainConsumeSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"messages":[],"next_offset":0,"count":0}`)
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_CONSUME") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "consume", "-topic", "test", "-limit", "1"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainConsumeSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_CONSUME=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("consume failed: %v\noutput: %s", err, string(output))
	}
	// Empty message list is fine — we at least exercised the dispatch branch
}

func TestMainBackupHelpSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_BACKUP_HELP") == "1" {
		os.Args = []string{"chimera", "backup", "--help"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainBackupHelpSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_BACKUP_HELP=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("backup --help failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "backup") {
		t.Errorf("expected help output, got: %s", string(output))
	}
}

func TestMainRestoreHelpSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_RESTORE_HELP") == "1" {
		os.Args = []string{"chimera", "restore", "--help"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainRestoreHelpSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_RESTORE_HELP=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore --help failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "restore") {
		t.Errorf("expected help output, got: %s", string(output))
	}
}

func TestMainUpgradeHelpSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_UPGRADE_HELP") == "1" {
		os.Args = []string{"chimera", "upgrade", "--help"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainUpgradeHelpSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_UPGRADE_HELP=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("upgrade --help failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "upgrade") {
		t.Errorf("expected help output, got: %s", string(output))
	}
}

func TestMainReloadSubprocess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/v1/config/reload" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"reloaded"}`)
		}
	}))
	defer server.Close()

	if os.Getenv("TEST_MAIN_RELOAD") == "1" {
		os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
		os.Args = []string{"chimera", "reload"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainReloadSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_RELOAD=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("reload failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "reloaded") {
		t.Errorf("expected reloaded in output, got: %s", string(output))
	}
}

func TestMainMCPServerConfigErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_MCP_CONFIG_ERR") == "1" {
		os.Args = []string{"chimera", "mcp-server", "--config", "/nonexistent/config.yaml"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainMCPServerConfigErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_MCP_CONFIG_ERR=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for bad config")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestMainMCPServerServeSubprocess(t *testing.T) {
	if os.Getenv("TEST_MAIN_MCP_SERVE") == "1" {
		r, w, _ := os.Pipe()
		os.Stdin = r
		w.Close()
		os.Args = []string{"chimera", "mcp-server"}
		main()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainMCPServerServeSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MAIN_MCP_SERVE=1", "CHIMERA_DATA_DIR="+t.TempDir())
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mcp-server failed: %v\noutput: %s", err, string(output))
	}
}

// --- Direct tests for main() command dispatch ---

func TestMainDispatchTopicList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/topics" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"topics":["t1"]}`)
		}
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "topic", "list"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "t1") {
		t.Errorf("expected topic in output, got: %s", output)
	}
}

func TestMainDispatchClusterStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/health" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"ok"}`)
		}
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "cluster", "status"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "ok") {
		t.Errorf("expected status in output, got: %s", output)
	}
}

func TestMainDispatchWASMList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/wasm/modules" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"modules":["mod1"]}`)
		}
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "wasm", "list"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "mod1") {
		t.Errorf("expected module in output, got: %s", output)
	}
}

func TestMainDispatchProduce(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"partition":0,"offset":1}`)
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "produce", "-topic", "test", "-message", "hello"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "Published") {
		t.Errorf("expected published message in output, got: %s", output)
	}
}

func TestMainDispatchConsume(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"messages":[],"next_offset":0,"count":0}`)
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "consume", "-topic", "test", "-limit", "1"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	// Empty message list is fine — we exercised the dispatch branch
	_ = rOut
}

func TestMainDispatchReload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/v1/config/reload" {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"reloaded"}`)
		}
	}))
	defer server.Close()

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldEnv := os.Getenv("CHIMERA_ADMIN_ADDR")
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Setenv("CHIMERA_ADMIN_ADDR", oldEnv)
	}()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	os.Args = []string{"chimera", "reload"}

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	main()

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "reloaded") {
		t.Errorf("expected reloaded in output, got: %s", output)
	}
}

func TestMainDispatchMCPServer(t *testing.T) {
	oldArgs := os.Args
	oldStdin := os.Stdin
	defer func() {
		os.Args = oldArgs
		os.Stdin = oldStdin
	}()

	r, w, _ := os.Pipe()
	os.Stdin = r
	w.Close()

	os.Args = []string{"chimera", "mcp-server"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		main()
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(2 * time.Second):
		t.Fatal("mcp-server dispatch did not return within timeout")
	}
}
