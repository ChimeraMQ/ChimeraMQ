package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateBackupAndRestore(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create some files
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0755)
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0644)
	os.WriteFile(filepath.Join(srcDir, "subdir", "b.txt"), []byte("world"), 0644)

	backupPath := filepath.Join(t.TempDir(), "test.tar.gz")

	if err := createBackup(srcDir, backupPath); err != nil {
		t.Fatalf("createBackup failed: %v", err)
	}

	if err := restoreBackup(backupPath, dstDir); err != nil {
		t.Fatalf("restoreBackup failed: %v", err)
	}

	// Verify restored files
	aData, err := os.ReadFile(filepath.Join(dstDir, "a.txt"))
	if err != nil {
		t.Fatalf("read restored a.txt: %v", err)
	}
	if string(aData) != "hello" {
		t.Errorf("a.txt = %q, want hello", string(aData))
	}

	bData, err := os.ReadFile(filepath.Join(dstDir, "subdir", "b.txt"))
	if err != nil {
		t.Fatalf("read restored b.txt: %v", err)
	}
	if string(bData) != "world" {
		t.Errorf("b.txt = %q, want world", string(bData))
	}
}

func TestRestoreBackupPathTraversal(t *testing.T) {
	dstDir := t.TempDir()
	backupPath := filepath.Join(t.TempDir(), "evil.tar.gz")

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)

	header := &tar.Header{
		Name:     "../evil.txt",
		Mode:     0644,
		Size:     int64(len("bad")),
		Typeflag: tar.TypeReg,
	}
	tarWriter.WriteHeader(header)
	tarWriter.Write([]byte("bad"))
	tarWriter.Close()
	gzWriter.Close()

	os.WriteFile(backupPath, buf.Bytes(), 0644)

	err := restoreBackup(backupPath, dstDir)
	if err == nil {
		t.Fatal("expected error for path traversal in archive")
	}
}

func TestRunBackupCLI(t *testing.T) {
	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0644)

	backupPath := filepath.Join(t.TempDir(), "backup.tar.gz")

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunBackupCLI([]string{"-data-dir", srcDir, "-output", backupPath, "-v"})

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "Backing up") {
		t.Errorf("expected verbose output, got: %s", output)
	}

	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Fatalf("backup file not created: %s", backupPath)
	}
}

func TestRunRestoreCLI(t *testing.T) {
	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("hello"), 0644)

	backupPath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := createBackup(srcDir, backupPath); err != nil {
		t.Fatalf("createBackup failed: %v", err)
	}

	dstDir := t.TempDir()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunRestoreCLI([]string{"-input", backupPath, "-data-dir", dstDir, "-v"})

	w.Close()
	os.Stdout = oldStdout

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, "Restoring") {
		t.Errorf("expected verbose output, got: %s", output)
	}

	data, err := os.ReadFile(filepath.Join(dstDir, "a.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("restored data = %q, want hello", string(data))
	}
}

