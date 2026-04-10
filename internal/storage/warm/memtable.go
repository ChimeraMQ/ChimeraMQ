package warm

import (
	"bytes"
	"errors"
	"sync"
	"time"
)

var errMemTableFrozen = errors.New("memtable is frozen")

// MemTableEntry holds a key-value pair with tombstone support.
type MemTableEntry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Deleted   bool
}

// MemTable is an in-memory sorted map backed by a left-leaning red-black tree.
type MemTable struct {
	mu       sync.RWMutex
	root     *llrbNode
	size     int64
	capacity int64
	count    uint32
	frozen   bool
	created  time.Time
}

type llrbNode struct {
	key       []byte
	value     []byte
	timestamp int64
	deleted   bool
	left      *llrbNode
	right     *llrbNode
	red       bool
}

func newLLRBNode(key, value []byte, deleted bool) *llrbNode {
	return &llrbNode{
		key:       key,
		value:     value,
		timestamp: time.Now().UnixNano(),
		deleted:   deleted,
		red:       true,
	}
}

// NewMemTable creates a new MemTable with the given byte capacity.
func NewMemTable(capacity int64) *MemTable {
	if capacity <= 0 {
		capacity = 4 * 1024 * 1024 // 4MB default
	}
	return &MemTable{
		capacity: capacity,
		created:  time.Now(),
	}
}

// Put inserts or updates a key-value pair.
func (mt *MemTable) Put(key, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.frozen {
		return errMemTableFrozen
	}

	mt.root = mt.insert(mt.root, key, value, false)
	mt.root.red = false
	mt.size += int64(len(key) + len(value))
	mt.count++
	return nil
}

// Delete inserts a tombstone for the key.
func (mt *MemTable) Delete(key []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.frozen {
		return errMemTableFrozen
	}

	mt.root = mt.insert(mt.root, key, nil, true)
	mt.root.red = false
	mt.size += int64(len(key))
	mt.count++
	return nil
}

// Get returns the value for a key. Returns (value, found, deleted).
func (mt *MemTable) Get(key []byte) ([]byte, bool, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	node := mt.search(mt.root, key)
	if node == nil {
		return nil, false, false
	}
	return node.value, true, node.deleted
}

// Size returns the approximate byte size.
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// Count returns the number of entries.
func (mt *MemTable) Count() uint32 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.count
}

// IsFrozen returns whether the memtable is frozen (read-only).
func (mt *MemTable) IsFrozen() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.frozen
}

// Freeze marks the memtable as read-only.
func (mt *MemTable) Freeze() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.frozen = true
}

// AtCapacity returns true if the memtable has reached its capacity.
func (mt *MemTable) AtCapacity() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size >= mt.capacity
}

// Iterator returns an iterator over all entries in sorted key order.
func (mt *MemTable) Iterator() *MemTableIterator {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// In-order traversal to collect entries
	entries := make([]MemTableEntry, 0, mt.count)
	mt.inorder(mt.root, &entries)
	return &MemTableIterator{entries: entries, pos: -1}
}

// MemTableIterator iterates over memtable entries in sorted order.
type MemTableIterator struct {
	entries []MemTableEntry
	pos     int
}

// Next advances to the next entry. Returns false when done.
func (it *MemTableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

// Entry returns the current entry.
func (it *MemTableIterator) Entry() MemTableEntry {
	if it.pos < 0 || it.pos >= len(it.entries) {
		return MemTableEntry{}
	}
	return it.entries[it.pos]
}

// --- Left-Leaning Red-Black Tree operations ---

func (mt *MemTable) insert(h *llrbNode, key, value []byte, deleted bool) *llrbNode {
	if h == nil {
		return newLLRBNode(key, value, deleted)
	}

	cmp := bytes.Compare(key, h.key)
	switch {
	case cmp < 0:
		h.left = mt.insert(h.left, key, value, deleted)
	case cmp > 0:
		h.right = mt.insert(h.right, key, value, deleted)
	default:
		// Update existing
		h.value = value
		h.deleted = deleted
		h.timestamp = time.Now().UnixNano()
	}

	// Fix-up: right-leaning reds and two reds in a row on left
	if isRed(h.right) && !isRed(h.left) {
		h = rotateLeft(h)
	}
	if isRed(h.left) && isRed(h.left.left) {
		h = rotateRight(h)
	}
	if isRed(h.left) && isRed(h.right) {
		flipColors(h)
	}

	return h
}

func (mt *MemTable) search(h *llrbNode, key []byte) *llrbNode {
	for h != nil {
		cmp := bytes.Compare(key, h.key)
		switch {
		case cmp < 0:
			h = h.left
		case cmp > 0:
			h = h.right
		default:
			return h
		}
	}
	return nil
}

func (mt *MemTable) inorder(h *llrbNode, result *[]MemTableEntry) {
	if h == nil {
		return
	}
	mt.inorder(h.left, result)
	*result = append(*result, MemTableEntry{
		Key:       h.key,
		Value:     h.value,
		Timestamp: h.timestamp,
		Deleted:   h.deleted,
	})
	mt.inorder(h.right, result)
}

func isRed(n *llrbNode) bool {
	return n != nil && n.red
}

func rotateLeft(h *llrbNode) *llrbNode {
	x := h.right
	h.right = x.left
	x.left = h
	x.red = h.red
	h.red = true
	return x
}

func rotateRight(h *llrbNode) *llrbNode {
	x := h.left
	h.left = x.right
	x.right = h
	x.red = h.red
	h.red = true
	return x
}

func flipColors(h *llrbNode) {
	h.red = !h.red
	h.left.red = !h.left.red
	h.right.red = !h.right.red
}
