package warm

import (
	"encoding/binary"
	"math"
)

// BloomFilter is a probabilistic data structure for set membership testing.
// Uses Kirsch-Mitzenmacker double-hashing to generate K hash values from two base hashes.
type BloomFilter struct {
	bits    []uint64
	numBits uint32
	numHash uint32
}

// NewBloomFilter creates a bloom filter sized for expectedItems with the given false positive rate.
func NewBloomFilter(expectedItems uint32, fpRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1000
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}

	// Optimal number of bits: m = -n * ln(p) / (ln2)^2
	numBits := uint32(float64(expectedItems) * -math.Log(fpRate) / (math.Ln2 * math.Ln2))
	if numBits < 64 {
		numBits = 64
	}
	// Round up to multiple of 64
	numBits = (numBits + 63) &^ 63

	// Optimal number of hash functions: k = (m/n) * ln2
	numHash := uint32(float64(numBits)/float64(expectedItems)*math.Ln2 + 0.5)
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 30 {
		numHash = 30
	}

	return &BloomFilter{
		bits:    make([]uint64, numBits/64),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hashKernel(key)
	for i := uint32(0); i < bf.numHash; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.numBits)
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
}

// MayContain returns true if the key might be in the set, false if definitely not.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hashKernel(key)
	for i := uint32(0); i < bf.numHash; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.numBits)
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// Size returns the number of bits in the filter.
func (bf *BloomFilter) Size() int {
	return int(bf.numBits)
}

// NumHash returns the number of hash functions used.
func (bf *BloomFilter) NumHash() uint32 {
	return bf.numHash
}

// MarshalBinary serializes the bloom filter to binary.
// Format: [version:1][numBits:4][numHash:4][bits...]
func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	// 1 + 4 + 4 + len(bits)*8
	size := 9 + len(bf.bits)*8
	buf := make([]byte, size)
	buf[0] = 1 // version
	binary.BigEndian.PutUint32(buf[1:5], bf.numBits)
	binary.BigEndian.PutUint32(buf[5:9], bf.numHash)
	for i, word := range bf.bits {
		binary.BigEndian.PutUint64(buf[9+i*8:], word)
	}
	return buf, nil
}

// UnmarshalBinary deserializes a bloom filter from binary.
func (bf *BloomFilter) UnmarshalBinary(data []byte) error {
	if len(data) < 9 || data[0] != 1 {
		return errBadBloomData
	}
	bf.numBits = binary.BigEndian.Uint32(data[1:5])
	bf.numHash = binary.BigEndian.Uint32(data[5:9])
	numWords := bf.numBits / 64
	if len(data) < int(9+numWords*8) {
		return errBadBloomData
	}
	bf.bits = make([]uint64, numWords)
	for i := uint32(0); i < numWords; i++ {
		bf.bits[i] = binary.BigEndian.Uint64(data[9+i*8:])
	}
	return nil
}

// hashKernel returns two independent hash values using murmur3-style mixing.
func hashKernel(key []byte) (uint64, uint64) {
	h1 := murmurMix(hashBytes(key, 0))
	h2 := murmurMix(hashBytes(key, 1))
	return h1, h2
}

// hashBytes produces a hash of key with the given seed.
func hashBytes(key []byte, seed uint8) uint64 {
	var h = uint64(seed)*0x9e3779b97f4a7c15 + 0x517cc1b727220a95
	for _, b := range key {
		h ^= uint64(b)
		h *= 0x5bd1e9955bd1e995
		h ^= h >> 15
	}
	return h
}

// murmurMix is the finalization mix from murmur3.
func murmurMix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
