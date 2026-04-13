package idempotent

import (
	"testing"
	"time"
)

func TestEvictOldest(t *testing.T) {
	d := NewDeduper(Config{Enabled: true, MaxEntries: 4})
	now := time.Now()

	// Add 4 entries with different times
	d.mu.Lock()
	w := d.windows["p1"]
	if w == nil {
		w = &dedupWindow{seen: make(map[string]time.Time)}
		d.windows["p1"] = w
	}
	w.seen["a"] = now.Add(-4 * time.Second)
	w.seen["b"] = now.Add(-3 * time.Second)
	w.seen["c"] = now.Add(-2 * time.Second)
	w.seen["d"] = now.Add(-1 * time.Second)
	d.mu.Unlock()

	d.mu.Lock()
	d.evictOldest(d.windows["p1"])
	d.mu.Unlock()

	if len(w.seen) != 3 {
		t.Errorf("expected 3 entries after eviction, got %d", len(w.seen))
	}
}

func TestEvictOldestProducer(t *testing.T) {
	d := NewDeduper(Config{Enabled: true})
	now := time.Now()

	d.mu.Lock()
	d.windows["p1"] = &dedupWindow{seen: make(map[string]time.Time), lastActivity: now.Add(-2 * time.Hour)}
	d.windows["p2"] = &dedupWindow{seen: make(map[string]time.Time), lastActivity: now.Add(-1 * time.Hour)}
	d.windows["p3"] = &dedupWindow{seen: make(map[string]time.Time), lastActivity: now.Add(-3 * time.Hour)}
	d.mu.Unlock()

	d.mu.Lock()
	d.evictOldestProducer(now)
	d.mu.Unlock()

	if d.ProducerCount() != 2 {
		t.Errorf("expected 2 producers after eviction, got %d", d.ProducerCount())
	}

	// p3 should be evicted (oldest activity)
	if d.LastSequence("p3") != 0 {
		t.Error("p3 should have been evicted")
	}
}

func TestEvictExpiredInactiveProducers(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		WindowSize: time.Hour,
	})
	now := time.Now()

	d.mu.Lock()
	d.windows["active"] = &dedupWindow{
		seen:         map[string]time.Time{"key": now},
		lastActivity: now,
	}
	d.windows["inactive"] = &dedupWindow{
		seen:         map[string]time.Time{"key": now.Add(-48 * time.Hour)},
		lastActivity: now.Add(-48 * time.Hour),
	}
	d.mu.Unlock()

	d.EvictExpired()

	if d.ProducerCount() != 1 {
		t.Errorf("expected 1 producer after eviction, got %d", d.ProducerCount())
	}

	if d.LastSequence("inactive") != 0 {
		t.Error("inactive producer should have been evicted")
	}
}

func TestCheckEvictExpiredAtCapacity(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		MaxEntries: 3,
		WindowSize: time.Hour,
	})
	now := time.Now()

	d.mu.Lock()
	w := &dedupWindow{seen: make(map[string]time.Time)}
	w.seen["old1"] = now.Add(-2 * time.Hour)
	w.seen["old2"] = now.Add(-2 * time.Hour)
	w.seen["recent"] = now
	d.windows["p1"] = w
	d.mu.Unlock()

	// Adding a new entry should trigger evictExpired since len(seen) == maxEntries
	// but old1/old2 are expired, so they get removed and new entry fits
	if d.Check("p1", 99, "new") {
		t.Error("new entry should not be duplicate")
	}

	// After eviction of expired entries, we should have recent + new
	d.mu.RLock()
	count := len(d.windows["p1"].seen)
	d.mu.RUnlock()
	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}
}

func TestCheckEvictOldestAtCapacity(t *testing.T) {
	d := NewDeduper(Config{
		Enabled:    true,
		MaxEntries: 3,
		WindowSize: time.Hour,
	})
	now := time.Now()

	d.mu.Lock()
	w := &dedupWindow{seen: make(map[string]time.Time)}
	w.seen["a"] = now.Add(-10 * time.Second)
	w.seen["b"] = now.Add(-5 * time.Second)
	w.seen["c"] = now.Add(-1 * time.Second)
	d.windows["p1"] = w
	d.mu.Unlock()

	// All entries are within window, so adding new entry triggers evictOldest
	if d.Check("p1", 99, "d") {
		t.Error("new entry should not be duplicate")
	}

	d.mu.RLock()
	count := len(d.windows["p1"].seen)
	d.mu.RUnlock()
	if count != 3 {
		t.Errorf("expected 3 entries after oldest eviction, got %d", count)
	}
}
