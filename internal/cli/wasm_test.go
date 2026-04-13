package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

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
