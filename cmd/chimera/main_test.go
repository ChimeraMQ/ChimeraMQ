package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
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
