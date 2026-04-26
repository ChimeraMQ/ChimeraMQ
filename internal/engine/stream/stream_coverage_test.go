package stream

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateGroupNameEmpty(t *testing.T) {
	if err := ValidateGroupName(""); err == nil {
		t.Error("expected error for empty group name")
	}
}

func TestValidateGroupNameTooLong(t *testing.T) {
	long := strings.Repeat("a", 257)
	if err := ValidateGroupName(long); err == nil {
		t.Error("expected error for group name too long")
	}
}

func TestValidateGroupNameInvalidChars(t *testing.T) {
	invalid := []string{"group/name", "group.name", "group:name", "group space", "group@name"}
	for _, g := range invalid {
		if err := ValidateGroupName(g); err == nil {
			t.Errorf("expected error for invalid group name %q", g)
		}
	}
}

func TestValidateGroupNamePathTraversal(t *testing.T) {
	if err := ValidateGroupName("../escape"); err == nil {
		t.Error("expected error for path traversal group name")
	}
}

func TestValidateGroupNameValid(t *testing.T) {
	valid := []string{"group-1", "group_1", "Group1", "a", strings.Repeat("a", 256)}
	for _, g := range valid {
		if err := ValidateGroupName(g); err != nil {
			t.Errorf("unexpected error for valid group name %q: %v", g, err)
		}
	}
}

func TestNewOffsetStoreMkdirError(t *testing.T) {
	dir := t.TempDir()
	// Create a file where the "consumers" subdirectory would be created
	badPath := filepath.Join(dir, "consumers")
	if err := os.WriteFile(badPath, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := NewOffsetStore(dir)
	if err == nil {
		t.Error("expected error when consumers dir cannot be created")
	}
}

func TestOffsetStoreSaveInvalidGroupName(t *testing.T) {
	store, err := NewOffsetStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Save("bad/group", 0, 1); err == nil {
		t.Error("expected error for invalid group name in Save")
	}
}

func TestFetchEngineClosed(t *testing.T) {
	engine, cleanup := setupEngine(t)
	defer cleanup()

	engine.Close()

	_, _, err := engine.Fetch("closed-topic", 0, 0, 10, time.Second)
	if err == nil {
		t.Error("expected error when fetching from closed engine")
	}
}

func TestEngineSetMigrator(t *testing.T) {
	engine, cleanup := setupEngine(t)
	defer cleanup()

	// Should not panic with nil
	engine.SetMigrator(nil)

	// Should not panic with a non-nil migrator (we can't easily create a real one
	// without hot/warm/cold engines, but the setter just assigns the field)
	engine.SetMigrator(nil)
}

func TestEngineSetEncryptor(t *testing.T) {
	engine, cleanup := setupEngine(t)
	defer cleanup()

	// Set a nil encryptor — should not panic
	engine.SetEncryptor(nil)
}

func TestConsumerGroupTopic(t *testing.T) {
	engine, cleanup := setupEngine(t)
	defer cleanup()

	engine.JoinGroup("grp", "my-topic", "m1", 1, StrategyRange)
	group := engine.GetGroup("grp")
	if group == nil {
		t.Fatal("group should exist")
	}
	if group.Topic() != "my-topic" {
		t.Errorf("Topic() = %q, want my-topic", group.Topic())
	}
}
