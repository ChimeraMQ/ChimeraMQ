package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
