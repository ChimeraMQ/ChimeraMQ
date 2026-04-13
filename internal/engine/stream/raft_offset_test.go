package stream

import (
	"testing"
)

func TestRaftOffsetStoreLocalOnly(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRaftOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("NewRaftOffsetStore: %v", err)
	}

	// Save and Get
	if err := store.Save("group1", 0, 42); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got := store.Get("group1", 0); got != 42 {
		t.Errorf("Get = %d, want 42", got)
	}

	// Get non-existent
	if got := store.Get("group1", 99); got != 0 {
		t.Errorf("Get non-existent = %d, want 0", got)
	}
}

func TestRaftOffsetStoreApplyOffset(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRaftOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("NewRaftOffsetStore: %v", err)
	}

	if err := store.ApplyOffset("group2", 1, 100); err != nil {
		t.Fatalf("ApplyOffset: %v", err)
	}

	if got := store.Get("group2", 1); got != 100 {
		t.Errorf("Get = %d, want 100", got)
	}
}

func TestRaftOffsetStoreMultipleGroupsAndPartitions(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRaftOffsetStore(dir, nil)
	if err != nil {
		t.Fatalf("NewRaftOffsetStore: %v", err)
	}

	for _, tc := range []struct {
		group string
		part  uint32
		off   uint64
	}{
		{"g1", 0, 10},
		{"g1", 1, 20},
		{"g2", 0, 30},
	} {
		if err := store.Save(tc.group, tc.part, tc.off); err != nil {
			t.Fatalf("Save %s/%d: %v", tc.group, tc.part, err)
		}
	}

	for _, tc := range []struct {
		group string
		part  uint32
		want  uint64
	}{
		{"g1", 0, 10},
		{"g1", 1, 20},
		{"g2", 0, 30},
		{"g2", 1, 0},
	} {
		if got := store.Get(tc.group, tc.part); got != tc.want {
			t.Errorf("Get %s/%d = %d, want %d", tc.group, tc.part, got, tc.want)
		}
	}
}
