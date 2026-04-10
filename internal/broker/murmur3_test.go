package broker

import "testing"

func TestMurmur3Deterministic(t *testing.T) {
	data := []byte("hello chimera")
	h1 := murmur3Hash(data)
	h2 := murmur3Hash(data)
	if h1 != h2 {
		t.Errorf("hash not deterministic: %d vs %d", h1, h2)
	}
}

func TestMurmur3DifferentInputs(t *testing.T) {
	h1 := murmur3Hash([]byte("key1"))
	h2 := murmur3Hash([]byte("key2"))
	if h1 == h2 {
		t.Error("different inputs should produce different hashes")
	}
}

func TestMurmur3Empty(t *testing.T) {
	h := murmur3Hash([]byte{})
	// Empty string hash is valid; just verify it doesn't panic
	_ = h
}

func TestMurmur3Distribution(t *testing.T) {
	buckets := make(map[uint32]int)
	for i := 0; i < 1000; i++ {
		key := []byte("key-" + string(rune(i)))
		h := murmur3Hash(key)
		buckets[h%16]++
	}
	// With 1000 keys into 16 buckets, each should get ~62
	for b, count := range buckets {
		if count < 20 || count > 150 {
			t.Errorf("bucket %d has %d items, expected ~62", b, count)
		}
	}
}
