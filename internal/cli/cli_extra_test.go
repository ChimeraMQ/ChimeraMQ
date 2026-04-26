package cli

import (
	"archive/tar"
	"compress/gzip"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// --- RunReloadCLI error paths (subprocess tests) ---

func TestRunReloadCLIErrorStatus(t *testing.T) {
	testSubprocess(t, "TestRunReloadCLIErrorStatusRunner")
}

func TestRunReloadCLIErrorStatusRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"config reload failed"}`))
	}))
	defer server.Close()
	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	RunReloadCLI([]string{})
}

func TestRunReloadCLIConnectionError(t *testing.T) {
	testSubprocess(t, "TestRunReloadCLIConnectionErrorRunner")
}

func TestRunReloadCLIConnectionErrorRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	os.Setenv("CHIMERA_ADMIN_ADDR", "http://localhost:1")
	RunReloadCLI([]string{})
}

// --- RunRestoreCLI paths ---

func TestRunRestoreCLIMissingInput(t *testing.T) {
	testSubprocess(t, "TestRunRestoreCLIMissingInputRunner")
}

func TestRunRestoreCLIMissingInputRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	RunRestoreCLI([]string{})
}

func TestRunRestoreCLINonForceNotEmpty(t *testing.T) {
	testSubprocess(t, "TestRunRestoreCLINonForceNotEmptyRunner")
}

func TestRunRestoreCLINonForceNotEmptyRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	dataDir := os.Getenv("TEST_DATADIR")
	os.WriteFile(dataDir+"/existing.txt", []byte("data"), 0644)
	backupPath := dataDir + "/backup.tar.gz"
	createTestTarGzOrDie(backupPath, map[string][]byte{"test": []byte("data")})
	RunRestoreCLI([]string{"-input", backupPath, "-data-dir", dataDir})
}

func TestRunRestoreCLINonExistentBackup(t *testing.T) {
	testSubprocess(t, "TestRunRestoreCLINonExistentBackupRunner")
}

func TestRunRestoreCLINonExistentBackupRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	RunRestoreCLI([]string{"-input", "/nonexistent/backup.tar.gz", "-data-dir", "/tmp", "-force"})
}

func TestRunRestoreCLIInvalidTarGz(t *testing.T) {
	testSubprocess(t, "TestRunRestoreCLIInvalidTarGzRunner")
}

func TestRunRestoreCLIInvalidTarGzRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	dataDir := os.Getenv("TEST_DATADIR")
	badPath := dataDir + "/bad.tar.gz"
	os.WriteFile(badPath, []byte("this is not gzip"), 0644)
	RunRestoreCLI([]string{"-input", badPath, "-data-dir", dataDir, "-force"})
}

// --- RunRestoreCLI success paths (direct tests) ---

func TestRunRestoreCLIForce(t *testing.T) {
	dataDir := t.TempDir()
	os.WriteFile(dataDir+"/existing.txt", []byte("data"), 0644)

	backupPath := dataDir + "/backup.tar.gz"
	createTestTarGz(t, backupPath, map[string][]byte{"wal/00001.log": []byte("wal data")})

	RunRestoreCLI([]string{"-input", backupPath, "-data-dir", dataDir, "-force"})

	if _, err := os.Stat(dataDir + "/wal/00001.log"); err != nil {
		t.Fatalf("expected restored file to exist: %v", err)
	}
}

func TestRunRestoreCLIEmptyDir(t *testing.T) {
	dataDir := t.TempDir()

	// Create backup in a separate temp dir so data dir stays empty
	backupDir := t.TempDir()
	backupPath := backupDir + "/backup.tar.gz"
	createTestTarGz(t, backupPath, map[string][]byte{"hot/segment.db": []byte("segment data")})

	RunRestoreCLI([]string{"-input", backupPath, "-data-dir", dataDir})

	if _, err := os.Stat(dataDir + "/hot/segment.db"); err != nil {
		t.Fatalf("expected restored file to exist: %v", err)
	}
}

// --- RunBackupCLI paths ---

func TestRunBackupCLINonexistentDataDir(t *testing.T) {
	testSubprocess(t, "TestRunBackupCLINonexistentDataDirRunner")
}

func TestRunBackupCLINonexistentDataDirRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	RunBackupCLI([]string{"-data-dir", "/nonexistent/chimera/data"})
}

func TestRunBackupCLICustomOutput(t *testing.T) {
	dataDir := t.TempDir()
	os.MkdirAll(dataDir+"/wal", 0750)
	os.WriteFile(dataDir+"/wal/00001.log", []byte("test data"), 0644)

	outputFile := dataDir + "/custom-backup.tar.gz"

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunBackupCLI([]string{"-data-dir", dataDir, "-output", outputFile})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "custom-backup.tar.gz") {
		t.Errorf("expected output filename in stdout, got: %s", output)
	}
	if _, err := os.Stat(outputFile); err != nil {
		t.Fatalf("expected backup file to exist: %v", err)
	}
}

func TestRunBackupCLIVerbose(t *testing.T) {
	dataDir := t.TempDir()
	os.MkdirAll(dataDir+"/hot", 0750)
	os.WriteFile(dataDir+"/hot/segment.db", []byte("data"), 0644)

	outputFile := dataDir + "/verbose-backup.tar.gz"

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunBackupCLI([]string{"-data-dir", dataDir, "-output", outputFile, "-v"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "Backing up") {
		t.Errorf("expected verbose output, got: %s", output)
	}
}

// --- createBackup/restoreBackup unit tests ---

func TestCreateBackupWalkError(t *testing.T) {
	// Use a path that will definitely fail on any OS
	err := createBackup(string([]byte{0}), t.TempDir()+"/out.tar.gz")
	if err == nil {
		t.Error("expected error for invalid data dir")
	}
}

func TestRestoreBackupOpenError(t *testing.T) {
	err := restoreBackup("/nonexistent/backup.tar.gz", t.TempDir())
	if err == nil {
		t.Error("expected error for missing backup file")
	}
}

func TestRestoreBackupDirEntries(t *testing.T) {
	dataDir := t.TempDir()

	backupPath := dataDir + "/dir-backup.tar.gz"
	f, err := os.Create(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	if err := tw.WriteHeader(&tar.Header{
		Name:     "subdir/",
		Mode:     0750,
		Typeflag: tar.TypeDir,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name:     "subdir/file.txt",
		Mode:     0644,
		Size:     int64(len("content")),
		Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("content")); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	f.Close()

	err = restoreBackup(backupPath, dataDir+"/restored")
	if err != nil {
		t.Fatalf("restore error: %v", err)
	}

	if _, err := os.Stat(dataDir + "/restored/subdir"); err != nil {
		t.Fatalf("expected subdir to exist: %v", err)
	}
	if _, err := os.Stat(dataDir + "/restored/subdir/file.txt"); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}
}

// --- RunUpgradeCLI paths ---

func TestRunUpgradeCLIUnknownAction(t *testing.T) {
	testSubprocess(t, "TestRunUpgradeCLIUnknownActionRunner")
}

func TestRunUpgradeCLIUnknownActionRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	RunUpgradeCLI([]string{"-action", "bogus-action"})
}

func TestRunUpgradeCLIDrainVerbose(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := sockDir + "/handoff.sock"

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
		conn.Write([]byte("OK  "))
	}()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunUpgradeCLI([]string{"-action", "drain", "-data-dir", sockDir, "-v"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "Requesting connection drain") {
		t.Errorf("expected verbose drain output, got: %s", output)
	}
}

func TestRunUpgradeCLIWaitVerbose(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := sockDir + "/handoff.sock"

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
		conn.Write([]byte("DRAI"))
		buf := make([]byte, 4)
		conn.Read(buf)
	}()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunUpgradeCLI([]string{"-action", "wait", "-data-dir", sockDir, "-v", "-timeout", "5s"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "Waiting for handoff") {
		t.Errorf("expected verbose wait output, got: %s", output)
	}
	if !strings.Contains(output, "Received handoff signal") {
		t.Errorf("expected handoff signal message, got: %s", output)
	}
}

// --- runWASMDeploy error paths ---

func TestRunWASMDeployFileNotFound(t *testing.T) {
	testSubprocess(t, "TestRunWASMDeployFileNotFoundRunner")
}

func TestRunWASMDeployFileNotFoundRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	runWASMDeploy([]string{"/nonexistent/module.wasm"})
}

func TestRunWASMDeployHTTPError(t *testing.T) {
	testSubprocess(t, "TestRunWASMDeployHTTPErrorRunner")
}

func TestRunWASMDeployHTTPErrorRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid module"}`))
	}))
	defer server.Close()
	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)

	tmpFile, _ := os.CreateTemp("", "test-*.wasm")
	tmpFile.Write([]byte("\x00asm"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())
	runWASMDeploy([]string{tmpFile.Name()})
}

func TestRunWASMDeployConnectionError(t *testing.T) {
	testSubprocess(t, "TestRunWASMDeployConnectionErrorRunner")
}

func TestRunWASMDeployConnectionErrorRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	os.Setenv("CHIMERA_ADMIN_ADDR", "http://localhost:1")

	tmpFile, _ := os.CreateTemp("", "test-*.wasm")
	tmpFile.Write([]byte("\x00asm"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())
	runWASMDeploy([]string{tmpFile.Name()})
}

func TestRunWASMDeployWithCustomName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"name":"my-custom-module"}`))
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	tmpFile, _ := os.CreateTemp("", "test-*.wasm")
	tmpFile.Write([]byte("\x00asm"))
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runWASMDeploy([]string{tmpFile.Name(), "--name", "my-custom-module"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "my-custom-module") {
		t.Errorf("expected custom name in output, got: %s", output)
	}
}

// --- runWASMList/Remove tests ---

func TestRunWASMListEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runWASMList()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "No WASM modules loaded") {
		t.Errorf("expected empty list message, got: %s", output)
	}
}

func TestRunWASMListWithModules(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"modules":["module-a.wasm","module-b.wasm"]}`))
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runWASMList()

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "WASM Modules (2)") {
		t.Errorf("expected module count in output, got: %s", output)
	}
}

func TestRunWASMRemoveSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	runWASMRemove([]string{"module-to-remove"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])

	if !strings.Contains(output, "removed") {
		t.Errorf("expected remove confirmation, got: %s", output)
	}
}

func TestRunWASMRemoveError(t *testing.T) {
	testSubprocess(t, "TestRunWASMRemoveErrorRunner")
}

func TestRunWASMRemoveErrorRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"module not found"}`))
	}))
	defer server.Close()
	os.Setenv("CHIMERA_ADMIN_ADDR", server.URL)
	runWASMRemove([]string{"nonexistent-module"})
}

func TestRunWASMRemoveMissingName(t *testing.T) {
	testSubprocess(t, "TestRunWASMRemoveMissingNameRunner")
}

func TestRunWASMRemoveMissingNameRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	runWASMRemove([]string{})
}

func TestRunWASMDeployMissingPath(t *testing.T) {
	testSubprocess(t, "TestRunWASMDeployMissingPathRunner")
}

func TestRunWASMDeployMissingPathRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	runWASMDeploy([]string{})
}

// --- RunMCPServer error paths ---

func TestRunMCPServerBadConfig(t *testing.T) {
	testSubprocess(t, "TestRunMCPServerBadConfigRunner")
}

func TestRunMCPServerBadConfigRunner(t *testing.T) {
	if os.Getenv("_CLI_SUBPROCESS") != "1" {
		return
	}
	RunMCPServer([]string{"--config", "/nonexistent/config.yaml"})
}

// --- Subprocess helper ---

func testSubprocess(t *testing.T, runnerTestName string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run="+runnerTestName)
	cmd.Env = append(os.Environ(), "_CLI_SUBPROCESS=1", "TEST_DATADIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected subprocess to exit non-zero, got output: %s", out)
	}
	// os.Exit(1) produces exit code 1
	// exec.ExitError is expected
}

// --- Helper test functions ---

func createTestTarGz(t *testing.T, path string, files map[string][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	for name, content := range files {
		h := &tar.Header{
			Name:     name,
			Mode:     0644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
}

func createTestTarGzOrDie(path string, files map[string][]byte) {
	f, _ := os.Create(path)
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	for name, content := range files {
		h := &tar.Header{
			Name:     name,
			Mode:     0644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}
		tw.WriteHeader(h)
		tw.Write(content)
	}
}
