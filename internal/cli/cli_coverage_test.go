package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Backup / Restore error paths ---

func TestCreateBackupNonExistentDir(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "test.tar.gz")
	err := createBackup("/nonexistent/dir/for/backup", backupPath)
	if err == nil {
		t.Error("expected error for non-existent data directory")
	}
}

func TestRestoreBackupInvalidGzip(t *testing.T) {
	invalidFile := filepath.Join(t.TempDir(), "not-a-backup.tar.gz")
	os.WriteFile(invalidFile, []byte("this is not gzip data"), 0644)

	err := restoreBackup(invalidFile, t.TempDir())
	if err == nil {
		t.Error("expected error for invalid gzip file")
	}
}

func TestRestoreBackupCorruptedTar(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "corrupt.tar.gz")

	file, _ := os.Create(backupPath)
	gzWriter := gzip.NewWriter(file)
	// Write partial gzip data that doesn't form a valid tar
	gzWriter.Write([]byte("not a valid tar archive"))
	gzWriter.Close()
	file.Close()

	err := restoreBackup(backupPath, t.TempDir())
	if err == nil {
		t.Error("expected error for corrupted tar archive")
	}
}

func TestRestoreBackupMkdirError(t *testing.T) {
	backupPath := filepath.Join(t.TempDir(), "backup.tar.gz")

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)

	header := &tar.Header{
		Name:     "file.txt",
		Mode:     0644,
		Size:     int64(len("data")),
		Typeflag: tar.TypeReg,
	}
	tarWriter.WriteHeader(header)
	tarWriter.Write([]byte("data"))
	tarWriter.Close()
	gzWriter.Close()
	os.WriteFile(backupPath, buf.Bytes(), 0644)

	// Use a file as the destination directory to cause MkdirAll to fail
	dstFile := filepath.Join(t.TempDir(), "notadir")
	os.WriteFile(dstFile, []byte("x"), 0644)

	err := restoreBackup(backupPath, filepath.Join(dstFile, "subdir"))
	if err == nil {
		t.Error("expected error when destination directory cannot be created")
	}
}

// --- Helpers ---

func TestReadStdin(t *testing.T) {
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	r, w, _ := os.Pipe()
	os.Stdin = r

	w.Write([]byte("hello stdin"))
	w.Close()

	data := readStdin()
	if string(data) != "hello stdin" {
		t.Errorf("readStdin = %q, want 'hello stdin'", string(data))
	}
}

func TestPrintWASMUsage(t *testing.T) {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printWASMUsage()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "deploy") {
		t.Errorf("expected usage to contain 'deploy', got: %s", output)
	}
}

// --- Produce / Consume CLI ---

func TestRunProduceCLIStdin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"partition":0,"offset":5}`)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	rIn, wIn, _ := os.Pipe()
	os.Stdin = rIn
	wIn.Write([]byte("stdin message"))
	wIn.Close()

	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	RunProduceCLI([]string{"-topic", "test-topic", "-count", "1"})

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "offset=5") {
		t.Errorf("expected 'offset=5' in output, got: %s", output)
	}
}

func TestRunProduceCLIMultipleMessages(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, fmt.Sprintf(`{"partition":0,"offset":%d}`, callCount))
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	oldStdout := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut

	RunProduceCLI([]string{"-topic", "test-topic", "-message", "hello", "-count", "3"})

	wOut.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := rOut.Read(buf)
	output := string(buf[:n])
	if strings.Count(output, "Published:") != 3 {
		t.Errorf("expected 3 published messages, got: %s", output)
	}
}

// --- Subprocess tests for os.Exit paths ---

func TestRunClusterCLIUnknownCommandSubprocess(t *testing.T) {
	if os.Getenv("TEST_CLUSTER_UNKNOWN_SUB") == "1" {
		RunClusterCLI([]string{"unknown"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunClusterCLIUnknownCommandSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_CLUSTER_UNKNOWN_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unknown command")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMCLIUnknownCommandSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_UNKNOWN_SUB") == "1" {
		RunWASMCLI([]string{"unknown-cmd"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMCLIUnknownCommandSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_UNKNOWN_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unknown command")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunUpgradeCLIUnknownActionSubprocess(t *testing.T) {
	if os.Getenv("TEST_UPGRADE_UNKNOWN_SUB") == "1" {
		RunUpgradeCLI([]string{"-action", "unknown"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunUpgradeCLIUnknownActionSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_UPGRADE_UNKNOWN_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unknown action")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunTopicCLIUnknownCommandSubprocess(t *testing.T) {
	if os.Getenv("TEST_TOPIC_UNKNOWN_SUB") == "1" {
		RunTopicCLI([]string{"unknown-cmd"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTopicCLIUnknownCommandSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_TOPIC_UNKNOWN_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unknown command")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunBackupCLISuccessSubprocess(t *testing.T) {
	if os.Getenv("TEST_BACKUP_SUB") == "1" {
		dir := os.Getenv("TEST_BACKUP_DIR")
		out := os.Getenv("TEST_BACKUP_OUT")
		RunBackupCLI([]string{"-data-dir", dir, "-output", out})
		return
	}

	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0644)
	backupPath := filepath.Join(t.TempDir(), "backup.tar.gz")

	cmd := exec.Command(os.Args[0], "-test.run=TestRunBackupCLISuccessSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_BACKUP_SUB=1", "TEST_BACKUP_DIR="+srcDir, "TEST_BACKUP_OUT="+backupPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("backup CLI failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "Backup created") {
		t.Errorf("expected success message, got: %s", string(output))
	}
}

func TestRunRestoreCLISuccessSubprocess(t *testing.T) {
	if os.Getenv("TEST_RESTORE_SUB") == "1" {
		input := os.Getenv("TEST_RESTORE_INPUT")
		dst := os.Getenv("TEST_RESTORE_DST")
		RunRestoreCLI([]string{"-input", input, "-data-dir", dst})
		return
	}

	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0644)

	backupPath := filepath.Join(t.TempDir(), "backup.tar.gz")
	err := createBackup(srcDir, backupPath)
	if err != nil {
		t.Fatal(err)
	}

	dstDir := filepath.Join(t.TempDir(), "restored")

	cmd := exec.Command(os.Args[0], "-test.run=TestRunRestoreCLISuccessSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_RESTORE_SUB=1", "TEST_RESTORE_INPUT="+backupPath, "TEST_RESTORE_DST="+dstDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore CLI failed: %v\noutput: %s", err, string(output))
	}
	if !strings.Contains(string(output), "Restore complete") {
		t.Errorf("expected success message, got: %s", string(output))
	}
}

// --- RunReloadCLI error path ---

func TestRunReloadCLIErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_RELOAD_ERROR_SUB") == "1" {
		RunReloadCLI([]string{})
		return
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":"reload failed"}`)
	}))
	defer server.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestRunReloadCLIErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_RELOAD_ERROR_SUB=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for reload failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- WASM error paths ---

func TestRunWASMRemoveNoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_REMOVE_NOARGS_SUB") == "1" {
		RunWASMCLI([]string{"remove"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMRemoveNoArgsSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_REMOVE_NOARGS_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing args")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMRemoveHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_REMOVE_HTTP_SUB") == "1" {
		RunWASMCLI([]string{"remove", "mod1"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMRemoveHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_REMOVE_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMRemoveErrorResponseSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_REMOVE_ERR_SUB") == "1" {
		RunWASMCLI([]string{"remove", "mod1"})
		return
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"error":"server error"}`)
	}))
	defer server.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMRemoveErrorResponseSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_REMOVE_ERR_SUB=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for error response")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMDeployNoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_DEPLOY_NOARGS_SUB") == "1" {
		RunWASMCLI([]string{"deploy"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMDeployNoArgsSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_DEPLOY_NOARGS_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing args")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMDeployFileNotFoundSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_DEPLOY_MISSING_SUB") == "1" {
		RunWASMCLI([]string{"deploy", "/nonexistent/path/file.wasm"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMDeployFileNotFoundSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_DEPLOY_MISSING_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing file")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMDeployErrorResponseSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_DEPLOY_ERR_SUB") == "1" {
		tmpFile, _ := os.CreateTemp("", "test-*.wasm")
		tmpFile.Write([]byte("\x00asm"))
		tmpFile.Close()
		defer os.Remove(tmpFile.Name())
		RunWASMCLI([]string{"deploy", tmpFile.Name()})
		return
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid wasm"}`)
	}))
	defer server.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMDeployErrorResponseSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_DEPLOY_ERR_SUB=1", "CHIMERA_ADMIN_ADDR="+server.URL)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for bad response")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunWASMCLINoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_NOARGS_SUB") == "1" {
		RunWASMCLI([]string{})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMCLINoArgsSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_NOARGS_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for no args")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- Topic CLI error paths ---

func TestRunTopicCLINoArgsSubprocess(t *testing.T) {
	if os.Getenv("TEST_TOPIC_NOARGS_SUB") == "1" {
		RunTopicCLI([]string{})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTopicCLINoArgsSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_TOPIC_NOARGS_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for no args")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunTopicCLIDescribeNoNameSubprocess(t *testing.T) {
	if os.Getenv("TEST_TOPIC_DESCRIBE_NONAME_SUB") == "1" {
		RunTopicCLI([]string{"describe"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTopicCLIDescribeNoNameSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_TOPIC_DESCRIBE_NONAME_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing name")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunTopicCLIDeleteNoNameSubprocess(t *testing.T) {
	if os.Getenv("TEST_TOPIC_DELETE_NONAME_SUB") == "1" {
		RunTopicCLI([]string{"delete"})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunTopicCLIDeleteNoNameSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_TOPIC_DELETE_NONAME_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing name")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- Produce / Consume error paths ---

func TestRunProduceCLIHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_PRODUCE_HTTP_SUB") == "1" {
		RunProduceCLI([]string{"-topic", "test", "-message", "hello"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunProduceCLIHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_PRODUCE_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunConsumeCLITopicRequiredSubprocess(t *testing.T) {
	if os.Getenv("TEST_CONSUME_NOTOPIC_SUB") == "1" {
		RunConsumeCLI([]string{})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestRunConsumeCLITopicRequiredSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_CONSUME_NOTOPIC_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for missing topic")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunConsumeCLIHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_CONSUME_HTTP_SUB") == "1" {
		RunConsumeCLI([]string{"-topic", "test", "-limit", "1"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunConsumeCLIHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_CONSUME_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- Upgrade helpers error paths ---

func TestGetHandoffStatusConnectionError(t *testing.T) {
	_, err := getHandoffStatus("/nonexistent/path/handoff.sock")
	if err == nil {
		t.Error("expected error for missing socket")
	}
}

func TestGetHandoffStatusReadError(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, "handoff.sock")

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Skipf("unix sockets not supported: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read STAT then close without writing
		buf := make([]byte, 4)
		conn.Read(buf)
		conn.Close()
	}()

	_, err = getHandoffStatus(sockPath)
	if err == nil {
		t.Error("expected error when server closes connection")
	}
}

func TestSendHandoffCommandConnectionError(t *testing.T) {
	err := sendHandoffCommand("/nonexistent/path/handoff.sock", "DRAI", 1*time.Second)
	if err == nil {
		t.Error("expected error for missing socket")
	}
}

func TestSendHandoffCommandShortResponse(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, "handoff.sock")

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Skipf("unix sockets not supported: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4)
		conn.Read(buf)
		conn.Write([]byte("OK")) // shorter than 4 bytes
	}()

	err = sendHandoffCommand(sockPath, "DRAI", 5*time.Second)
	if err == nil {
		t.Error("expected error for short response")
	}
}

// --- Upgrade CLI error paths ---

func TestRunUpgradeCLIStatusErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_UPGRADE_STATUS_ERR_SUB") == "1" {
		RunUpgradeCLI([]string{"-action", "status", "-data-dir", t.TempDir()})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunUpgradeCLIStatusErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_UPGRADE_STATUS_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for status failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunUpgradeCLIDrainErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_UPGRADE_DRAIN_ERR_SUB") == "1" {
		RunUpgradeCLI([]string{"-action", "drain", "-data-dir", t.TempDir()})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunUpgradeCLIDrainErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_UPGRADE_DRAIN_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for drain failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunUpgradeCLIWaitConnectErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_UPGRADE_WAIT_ERR_SUB") == "1" {
		RunUpgradeCLI([]string{"-action", "wait", "-data-dir", t.TempDir(), "-timeout", "1s"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunUpgradeCLIWaitConnectErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_UPGRADE_WAIT_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for wait failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunUpgradeCLIWaitUnexpectedSignalSubprocess(t *testing.T) {
	if os.Getenv("TEST_UPGRADE_WAIT_UNEXPECTED_SUB") == "1" {
		sockDir := os.Getenv("TEST_HANDOFF_DIR")
		RunUpgradeCLI([]string{"-action", "wait", "-data-dir", sockDir, "-timeout", "5s"})
		return
	}

	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, "handoff.sock")

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Skipf("unix sockets not supported: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Write([]byte("PING"))
			conn.Close()
		}
	}()

	cmd := exec.Command(os.Args[0], "-test.run=TestRunUpgradeCLIWaitUnexpectedSignalSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_UPGRADE_WAIT_UNEXPECTED_SUB=1", "TEST_HANDOFF_DIR="+sockDir)
	err = cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for unexpected signal")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- WASM list error path ---

func TestRunWASMListHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_WASM_LIST_HTTP_SUB") == "1" {
		RunWASMCLI([]string{"list"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunWASMListHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_WASM_LIST_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- MCP / Server error paths ---

func TestRunMCPServerConfigErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_MCP_CONFIG_ERR_SUB") == "1" {
		RunMCPServer([]string{"--config", "/nonexistent/config.yaml"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunMCPServerConfigErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MCP_CONFIG_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for bad config")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunServerConfigErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_SERVER_CONFIG_ERR_SUB") == "1" {
		RunServer([]string{"-config", "/nonexistent/config.yaml"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunServerConfigErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_SERVER_CONFIG_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for bad config")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunServerInvalidPortSubprocess(t *testing.T) {
	if os.Getenv("TEST_SERVER_PORT_ERR_SUB") == "1" {
		RunServer([]string{"-port", "99999"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunServerInvalidPortSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_SERVER_PORT_ERR_SUB=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for invalid port")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunServerStartErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_SERVER_START_ERR_SUB") == "1" {
		cfgPath := os.Getenv("TEST_SERVER_CONFIG_PATH")
		RunServer([]string{"-config", cfgPath})
		return
	}

	// Create a config that passes validation but fails during Start():
	// enable file auth with a non-existent auth file
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "chimera.yaml")
	cfgContent := `auth:
  enabled: true
  type: file
  auth_file: /nonexistent/auth.json
node:
  data_dir: ` + dir + `
`
	os.WriteFile(cfgPath, []byte(cfgContent), 0644)

	cmd := exec.Command(os.Args[0], "-test.run=TestRunServerStartErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_SERVER_START_ERR_SUB=1", "TEST_SERVER_CONFIG_PATH="+cfgPath)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for start failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

// --- Cluster CLI error paths ---

func TestRunClusterCLIStatusHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_CLUSTER_STATUS_HTTP_SUB") == "1" {
		RunClusterCLI([]string{"status"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunClusterCLIStatusHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_CLUSTER_STATUS_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunClusterCLIMembersHTTPErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_CLUSTER_MEMBERS_HTTP_SUB") == "1" {
		RunClusterCLI([]string{"members"})
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestRunClusterCLIMembersHTTPErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_CLUSTER_MEMBERS_HTTP_SUB=1", "CHIMERA_ADMIN_ADDR=http://127.0.0.1:1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for HTTP failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}

func TestRunMCPServerStartErrorSubprocess(t *testing.T) {
	if os.Getenv("TEST_MCP_START_ERR_SUB") == "1" {
		cfgPath := os.Getenv("TEST_MCP_CONFIG_PATH")
		RunMCPServer([]string{"--config", cfgPath})
		return
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "chimera.yaml")
	cfgContent := `auth:
  enabled: true
  type: file
  auth_file: /nonexistent/auth.json
node:
  data_dir: ` + dir + `
`
	os.WriteFile(cfgPath, []byte(cfgContent), 0644)

	cmd := exec.Command(os.Args[0], "-test.run=TestRunMCPServerStartErrorSubprocess", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_MCP_START_ERR_SUB=1", "TEST_MCP_CONFIG_PATH="+cfgPath)
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected exit error for start failure")
	}
	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}
