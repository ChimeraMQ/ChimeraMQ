package idempotent

import (
	"sync"
	"time"
)

// MaxProducers limits the number of tracked producers to prevent unbounded growth.
const MaxProducers = 100_000

// ProducerTTL is the time after which an inactive producer is evicted.
const ProducerTTL = 24 * time.Hour

// Deduper tracks recent message IDs per producer to enable idempotent publishing.
type Deduper struct {
	mu         sync.RWMutex
	windows    map[string]*dedupWindow // producerID → window
	windowSize time.Duration
	maxEntries int
	enabled    bool
}

type dedupWindow struct {
	seen         map[string]time.Time // messageKey → seen time
	lastSeq      uint64
	lastActivity time.Time // for producer-level TTL eviction
}

// Config holds deduplication configuration.
type Config struct {
	Enabled    bool
	WindowSize time.Duration // default: 5 minutes
	MaxEntries int           // per producer, default: 10000
}

// NewDeduper creates a new deduper.
func NewDeduper(cfg Config) *Deduper {
	ws := cfg.WindowSize
	if ws <= 0 {
		ws = 5 * time.Minute
	}
	max := cfg.MaxEntries
	if max <= 0 {
		max = 10000
	}
	return &Deduper{
		windows:    make(map[string]*dedupWindow),
		windowSize: ws,
		maxEntries: max,
		enabled:    cfg.Enabled,
	}
}

// Enabled returns whether deduplication is active.
func (d *Deduper) Enabled() bool { return d.enabled }

// Check returns true if the message is a duplicate (should be rejected).
// Returns false if the message is new (should be accepted).
// key is a unique identifier for the message (e.g., producerID + sequenceNumber).
func (d *Deduper) Check(producerID string, seq uint64, key string) bool {
	if !d.enabled {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	w, ok := d.windows[producerID]
	if !ok {
		// Check if we're at the producer limit
		if len(d.windows) >= MaxProducers {
			// Evict oldest inactive producer
			d.evictOldestProducer(now)
			// If still at limit, reject new producer
			if len(d.windows) >= MaxProducers {
				return false // accept but don't track (degraded mode)
			}
		}
		w = &dedupWindow{
			seen:         make(map[string]time.Time),
			lastActivity: now,
		}
		d.windows[producerID] = w
	}

	w.lastActivity = now

	// Check if we've seen this exact key
	if seenAt, exists := w.seen[key]; exists {
		if now.Sub(seenAt) < d.windowSize {
			return true // duplicate
		}
	}

	// Evict expired entries if at capacity
	if len(w.seen) >= d.maxEntries {
		d.evictExpired(w, now)
		if len(w.seen) >= d.maxEntries {
			// Still full after eviction — clear oldest entries
			d.evictOldest(w)
		}
	}

	// Record the new message
	w.seen[key] = now
	if seq > w.lastSeq {
		w.lastSeq = seq
	}
	return false
}

// IsDuplicate is an alias for Check with a composite key.
func (d *Deduper) IsDuplicate(producerID string, seq uint64) bool {
	return d.Check(producerID, seq, seqToKey(seq))
}

// LastSequence returns the highest sequence number seen for a producer.
func (d *Deduper) LastSequence(producerID string) uint64 {
	if !d.enabled {
		return 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	w, ok := d.windows[producerID]
	if !ok {
		return 0
	}
	return w.lastSeq
}

// RemoveProducer removes a producer's dedup window.
func (d *Deduper) RemoveProducer(producerID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.windows, producerID)
}

// ProducerCount returns the number of active producers.
func (d *Deduper) ProducerCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.windows)
}

// EvictExpired cleans up expired entries across all producers.
// Also removes producers that have been inactive for longer than ProducerTTL.
func (d *Deduper) EvictExpired() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()

	// Evict inactive producers
	for id, w := range d.windows {
		if now.Sub(w.lastActivity) > ProducerTTL {
			delete(d.windows, id)
		}
	}

	// Evict expired entries from remaining producers
	for _, w := range d.windows {
		d.evictExpired(w, now)
	}
}

func (d *Deduper) evictExpired(w *dedupWindow, now time.Time) {
	for k, seenAt := range w.seen {
		if now.Sub(seenAt) >= d.windowSize {
			delete(w.seen, k)
		}
	}
}

func (d *Deduper) evictOldest(w *dedupWindow) {
	// Remove 25% of entries (oldest)
	toRemove := d.maxEntries / 4
	if toRemove < 1 {
		toRemove = 1
	}
	var oldest []string
	oldestTime := time.Now()
	for k, seenAt := range w.seen {
		if seenAt.Before(oldestTime) {
			oldestTime = seenAt
			if len(oldest) < toRemove {
				oldest = append(oldest, k)
			}
		}
	}
	for _, k := range oldest {
		delete(w.seen, k)
	}
}

func seqToKey(seq uint64) string {
	// Fast uint64 to string without strconv
	if seq == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for seq > 0 {
		buf = append(buf, byte('0'+seq%10))
		seq /= 10
	}
	// Reverse
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

// evictOldestProducer removes the producer that has been inactive the longest.
func (d *Deduper) evictOldestProducer(now time.Time) {
	var oldestID string
	var oldestTime time.Time

	for id, w := range d.windows {
		if oldestTime.IsZero() || w.lastActivity.Before(oldestTime) {
			oldestTime = w.lastActivity
			oldestID = id
		}
	}

	if oldestID != "" {
		delete(d.windows, oldestID)
	}
}
