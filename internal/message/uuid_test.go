package message

import (
	"bytes"
	"slices"
	"testing"
)

func TestUUIDv7Uniqueness(t *testing.T) {
	seen := make(map[[16]byte]bool, 100_000)
	for i := 0; i < 100_000; i++ {
		u := NewUUIDv7()
		if seen[u] {
			t.Fatal("duplicate UUID generated")
		}
		seen[u] = true
	}
}

func TestUUIDv7MonotonicWithinMS(t *testing.T) {
	var last [16]byte
	for i := 0; i < 1000; i++ {
		u := NewUUIDv7()
		if i > 0 && bytes.Equal(last[6:8], u[6:8]) {
			// Same millisecond — counter should differ
			t.Error("counter not incrementing within same ms")
		}
		last = u
	}
}

func TestUUIDv7VersionBits(t *testing.T) {
	u := NewUUIDv7()
	if u[6]&0xF0 != 0x70 {
		t.Errorf("version bits should be 0x70, got %#x", u[6]&0xF0)
	}
}

func TestUUIDv7VariantBits(t *testing.T) {
	u := NewUUIDv7()
	if u[8]&0xC0 != 0x80 {
		t.Errorf("variant bits should be 0x80, got %#x", u[8]&0xC0)
	}
}

func TestUUIDString(t *testing.T) {
	u := NewUUIDv7()
	s := UUIDString(u)
	if len(s) != 36 {
		t.Errorf("expected 36 chars, got %d", len(s))
	}
	// Version nibble should be '7'
	if s[14] != '7' {
		t.Errorf("version nibble should be '7', got %c", s[14])
	}
	// Variant nibble should be 8, 9, a, or b
	v := s[19]
	if v != '8' && v != '9' && v != 'a' && v != 'b' {
		t.Errorf("variant nibble unexpected: %c", v)
	}
}

func TestUUIDv7TimeOrdered(t *testing.T) {
	gen := &UUIDv7Generator{}
	var uuids [100][16]byte
	for i := range uuids {
		uuids[i] = gen.Generate()
	}
	for i := 1; i < len(uuids); i++ {
		if slices.Compare(uuids[i-1][:], uuids[i][:]) > 0 {
			t.Error("UUIDs not monotonically increasing")
		}
	}
}

func BenchmarkUUIDv7(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewUUIDv7()
	}
}
