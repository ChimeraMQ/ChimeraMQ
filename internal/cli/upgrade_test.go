package cli

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGetHandoffStatus(t *testing.T) {
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
		if string(buf) == "STAT" {
			conn.Write([]byte("READY"))
		}
	}()

	status, err := getHandoffStatus(sockPath)
	if err != nil {
		t.Fatalf("getHandoffStatus: %v", err)
	}
	if status != "READY" {
		t.Errorf("status = %q, want READY", status)
	}
}

func TestSendHandoffCommandOK(t *testing.T) {
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
		conn.Write([]byte("OK  "))
	}()

	err = sendHandoffCommand(sockPath, "DRAI", 5*time.Second)
	if err != nil {
		t.Fatalf("sendHandoffCommand: %v", err)
	}
}

func TestSendHandoffCommandErr(t *testing.T) {
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
		conn.Write([]byte("ERR busy"))
	}()

	err = sendHandoffCommand(sockPath, "DRAI", 5*time.Second)
	if err == nil {
		t.Fatal("expected error for ERR response")
	}
}

func TestSendHandoffCommandUnexpected(t *testing.T) {
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
		conn.Write([]byte("WHAT"))
	}()

	err = sendHandoffCommand(sockPath, "DRAI", 5*time.Second)
	if err == nil {
		t.Fatal("expected error for unexpected response")
	}
}

func TestRunUpgradeCLIStatus(t *testing.T) {
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
		if string(buf) == "STAT" {
			conn.Write([]byte("ACTIVE"))
		}
	}()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunUpgradeCLI([]string{"-data-dir", sockDir})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !contains(output, "ACTIVE") {
		t.Errorf("expected ACTIVE in output, got: %s", output)
	}
}

func TestRunUpgradeCLIDrain(t *testing.T) {
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
		conn.Write([]byte("OK  "))
	}()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunUpgradeCLI([]string{"-action", "drain", "-data-dir", sockDir})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !contains(output, "initiated successfully") {
		t.Errorf("expected success message, got: %s", output)
	}
}

func TestRunUpgradeCLIWait(t *testing.T) {
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

		// Wait for client to connect and read
		conn.Write([]byte("DRAI"))
		buf := make([]byte, 4)
		conn.Read(buf)
	}()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	RunUpgradeCLI([]string{"-action", "wait", "-data-dir", sockDir, "-timeout", "5s"})

	w.Close()
	os.Stdout = old

	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	output := string(buf[:n])
	if !contains(output, "initiated successfully") {
		t.Errorf("expected success message, got: %s", output)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
