package idempotent

import (
	"testing"
	"time"
)

func TestNewDeduper(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	if !d.Enabled() {
		t.Error("should be enabled")
	}
}

func TestDeduperDisabled(t *testing.T) {
	d := NewDeduper(Config{})
	if d.Enabled() {
		t.Error("should be disabled")
	}
	if d.IsDuplicate("p1", 1) {
		t.Error("disabled deduper should never report duplicate")
	}
}

func TestNoDuplicate(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	if d.IsDuplicate("p1", 1) {
		t.Error("first message should not be duplicate")
	}
	if d.IsDuplicate("p1", 2) {
		t.Error("second message should not be duplicate")
	}
}

func TestDuplicate(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	d.IsDuplicate("p1", 1)
	if !d.IsDuplicate("p1", 1) {
		t.Error("same sequence should be duplicate")
	}
}

func TestDifferentProducers(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	d.IsDuplicate("p1", 1)
	if d.IsDuplicate("p2", 1) {
		t.Error("different producer should not be duplicate")
	}
}

func TestLastSequence(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	d.IsDuplicate("p1", 5)
	d.IsDuplicate("p1", 10)
	d.IsDuplicate("p1", 3)

	if d.LastSequence("p1") != 10 {
		t.Errorf("last seq = %d, want 10", d.LastSequence("p1"))
	}
}

func TestLastSequenceUnknown(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	if d.LastSequence("unknown") != 0 {
		t.Error("unknown producer should have 0 last seq")
	}
}

func TestLastSequenceDisabled(t *testing.T) {
	d := NewDeduper(Config{})
	if d.LastSequence("p1") != 0 {
		t.Error("disabled should always return 0")
	}
}

func TestRemoveProducer(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	d.IsDuplicate("p1", 1)
	d.RemoveProducer("p1")

	if d.IsDuplicate("p1", 1) {
		t.Error("should not be duplicate after producer removal")
	}
}

func TestProducerCount(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	d.IsDuplicate("p1", 1)
	d.IsDuplicate("p2", 1)
	d.IsDuplicate("p3", 1)

	if d.ProducerCount() != 3 {
		t.Errorf("count = %d, want 3", d.ProducerCount())
	}
}

func TestWindowExpiry(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		WindowSize: 50 * time.Millisecond,
	})
	d.IsDuplicate("p1", 1)

	// Wait for window to expire
	time.Sleep(80 * time.Millisecond)

	// Should no longer be duplicate
	if d.IsDuplicate("p1", 1) {
		t.Error("should not be duplicate after window expires")
	}
}

func TestEvictExpired(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		WindowSize: 50 * time.Millisecond,
	})
	d.IsDuplicate("p1", 1)
	time.Sleep(80 * time.Millisecond)

	d.EvictExpired()
	// Producer should still exist but with empty seen map
	if d.ProducerCount() != 1 {
		t.Error("producer should still exist")
	}
}

func TestCustomKey(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	if d.Check("p1", 1, "custom-key-abc") {
		t.Error("first check should not be duplicate")
	}
	if !d.Check("p1", 1, "custom-key-abc") {
		t.Error("same key should be duplicate")
	}
	if d.Check("p1", 2, "custom-key-xyz") {
		t.Error("different key should not be duplicate")
	}
}

func TestSeqToKey(t *testing.T) {
	tests := []struct {
		in   uint64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{123, "123"},
	}
	for _, tt := range tests {
		got := seqToKey(tt.in)
		if got != tt.want {
			t.Errorf("seqToKey(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestMaxEntries(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		MaxEntries: 5,
	})
	// Fill to capacity
	for i := uint64(0); i < 10; i++ {
		d.IsDuplicate("p1", i)
	}
	// Producer should still work after eviction
	if d.IsDuplicate("p1", 100) {
		t.Error("new seq should not be duplicate")
	}
}
