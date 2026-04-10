package warm

import (
	"fmt"
	"testing"
)

func TestBloomBasic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Empty filter: nothing should be found
	if bf.MayContain([]byte("hello")) {
		t.Error("empty filter should not contain anything")
	}

	// Add and verify
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))
	bf.Add([]byte("chimera"))

	if !bf.MayContain([]byte("hello")) {
		t.Error("should contain 'hello'")
	}
	if !bf.MayContain([]byte("world")) {
		t.Error("should contain 'world'")
	}
	if !bf.MayContain([]byte("chimera")) {
		t.Error("should contain 'chimera'")
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 10000
	bf := NewBloomFilter(n, 0.01)

	// Insert n items
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Verify all inserted items are found
	for i := 0; i < n; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Errorf("false negative for key-%d", i)
		}
	}

	// Measure false positive rate with non-members
	fpCount := 0
	testCount := 100000
	for i := 0; i < testCount; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("other-%d", i))) {
			fpCount++
		}
	}

	fpRate := float64(fpCount) / float64(testCount)
	if fpRate > 0.02 { // 2x the target 0.01
		t.Errorf("false positive rate = %.4f, want <= 0.02", fpRate)
	}
}

func TestBloomSerialization(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	data, err := bf.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	bf2 := &BloomFilter{}
	if err := bf2.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	if bf2.numBits != bf.numBits {
		t.Errorf("numBits mismatch: %d vs %d", bf2.numBits, bf.numBits)
	}
	if bf2.numHash != bf.numHash {
		t.Errorf("numHash mismatch: %d vs %d", bf2.numHash, bf.numHash)
	}

	if !bf2.MayContain([]byte("hello")) {
		t.Error("deserialized filter should contain 'hello'")
	}
	if !bf2.MayContain([]byte("world")) {
		t.Error("deserialized filter should contain 'world'")
	}
	if bf2.MayContain([]byte("nonexistent")) {
		t.Error("deserialized filter should not contain 'nonexistent'")
	}
}

func TestBloomZeroItems(t *testing.T) {
	bf := NewBloomFilter(0, 0.01)
	if bf.Size() == 0 {
		t.Error("filter should have some bits even with 0 expected items")
	}
}

func TestBloomLargeKey(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	largeKey := make([]byte, 1024)
	for i := range largeKey {
		largeKey[i] = byte(i)
	}
	bf.Add(largeKey)
	if !bf.MayContain(largeKey) {
		t.Error("should contain large key")
	}
}

func TestBloomNumHash(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	if bf.NumHash() < 1 || bf.NumHash() > 30 {
		t.Errorf("numHash = %d, want 1-30", bf.NumHash())
	}
}
