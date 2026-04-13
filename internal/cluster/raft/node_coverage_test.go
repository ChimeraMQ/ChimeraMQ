package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveStateMkdirFailure(t *testing.T) {
	dir := t.TempDir()
	// Place a file where the raft directory should be created
	blockDir := filepath.Join(dir, "raft")
	os.WriteFile(blockDir, []byte("x"), 0644)

	cfg := testConfig(t)
	cfg.DataDir = dir
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// saveState should fail silently (logs error) when MkdirAll fails
	node.saveState()
}

func TestSaveStateWriteFailure(t *testing.T) {
	dir := t.TempDir()
	// Create raft dir as a file so WriteFile fails
	raftDir := filepath.Join(dir, "raft")
	os.WriteFile(raftDir, []byte("x"), 0644)

	cfg := testConfig(t)
	cfg.DataDir = dir
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	node.saveState()
}

func TestSaveStateSuccess(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.DataDir = dir
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.currentTerm = 42
	node.votedFor = "node-5"
	node.mu.Unlock()

	node.saveState()

	statePath := filepath.Join(dir, "raft", "state.json")
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("state file should exist: %v", err)
	}
	if len(data) == 0 {
		t.Error("state file should not be empty")
	}
}
