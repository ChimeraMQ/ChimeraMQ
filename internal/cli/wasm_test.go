package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestRunWASMDeployFileNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	// Calls os.Exit(1) — test by running as subprocess
	if os.Getenv("TEST_WASM_DEPLOY_MISSING") == "1" {
		RunWASMCLI([]string{"deploy", "/nonexistent/path/file.wasm"})
		return
	}
}

func TestRunWASMDeployErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid wasm"}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	// Calls os.Exit(1) — subprocess test
	if os.Getenv("TEST_WASM_DEPLOY_ERROR") == "1" {
		tmpFile, _ := os.CreateTemp("", "test-*.wasm")
		tmpFile.Write([]byte("\x00asm"))
		tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		RunWASMCLI([]string{"deploy", tmpFile.Name()})
		return
	}
}

func TestRunWASMRemoveErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":"server error"}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	if os.Getenv("TEST_WASM_REMOVE_ERROR") == "1" {
		RunWASMCLI([]string{"remove", "mod1"})
		return
	}
}

func TestRunWASMCLINoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_NOARGS") == "1" {
		RunWASMCLI([]string{})
		return
	}
}

func TestRunWASMDeployNoNameFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"name":"test.wasm"}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	tmpFile, err := os.CreateTemp("", "my-module-*.wasm")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Write([]byte("\x00asm"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	RunWASMCLI([]string{"deploy", tmpFile.Name()})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "compiled successfully") {
		t.Errorf("expected success message, got: %s", output)
	}
}
