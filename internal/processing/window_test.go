package processing

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestTumblingWindow(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type: WindowTumbling,
		Size: 10 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	// Add events at t=1s and t=5s (same window)
	wm.AddEvent("k1", int64(time.Second), []byte("a"))
	wm.AddEvent("k1", int64(5*time.Second), []byte("b"))
	wm.AddEvent("k2", int64(3*time.Second), []byte("c"))

	if wm.WindowCount() != 1 {
		t.Errorf("WindowCount = %d, want 1", wm.WindowCount())
	}

	// Tick at t=11s — window [0,10s) should emit
	wm.Tick(int64(11 * time.Second))

	if emitted.Load() != 2 {
		t.Errorf("emitted = %d, want 2 (k1 and k2)", emitted.Load())
	}
	if wm.WindowCount() != 0 {
		t.Error("all windows should be closed")
	}
}

func TestTumblingWindowMultipleWindows(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type: WindowTumbling,
		Size: 5 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	wm.AddEvent("k1", int64(1*time.Second), []byte("a"))
	wm.AddEvent("k1", int64(7*time.Second), []byte("b"))

	if wm.WindowCount() != 2 {
		t.Errorf("WindowCount = %d, want 2", wm.WindowCount())
	}

	// Tick past both windows
	wm.Tick(int64(20 * time.Second))

	if emitted.Load() != 2 {
		t.Errorf("emitted = %d, want 2", emitted.Load())
	}
}

func TestTumblingWindowNotExpired(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type: WindowTumbling,
		Size: 10 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	wm.AddEvent("k1", int64(1*time.Second), []byte("a"))
	wm.Tick(int64(5 * time.Second)) // window not expired yet

	if emitted.Load() != 0 {
		t.Error("window should not have emitted yet")
	}
	if wm.WindowCount() != 1 {
		t.Error("window should still be open")
	}
}

func TestSlidingWindow(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type:  WindowSliding,
		Size:  10 * time.Second,
		Slide: 5 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	// Event at t=3s — belongs to windows [0,10) and [5,15)
	// Actually for sliding: event at 3s, windows starting at 0,5
	wm.AddEvent("k1", int64(3*time.Second), []byte("a"))

	// Should create 2 windows since slide=5s and the event at 3s
	// falls in both [0,10) and it's before [5,15)
	// Actually only [0,10) contains t=3s since [5,15) starts at 5s
	// Let me reconsider: event at 3s is in [0,10). Not in [5,15) since 3 < 5.
	// So only 1 window
	if wm.WindowCount() != 1 {
		t.Errorf("WindowCount = %d, want 1", wm.WindowCount())
	}

	// Event at t=6s — belongs to [0,10) and [5,15)
	wm.AddEvent("k1", int64(6*time.Second), []byte("b"))

	// Now we should have windows [0,10) and [5,15)
	if wm.WindowCount() != 2 {
		t.Errorf("WindowCount = %d, want 2", wm.WindowCount())
	}

	// Tick at 11s — [0,10) expires
	wm.Tick(int64(11 * time.Second))
	if emitted.Load() != 1 {
		t.Errorf("emitted = %d, want 1", emitted.Load())
	}
}

func TestSessionWindow(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type: WindowSession,
		Gap:  5 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	// Events within gap
	wm.AddEvent("k1", int64(1*time.Second), []byte("a"))
	wm.AddEvent("k1", int64(3*time.Second), []byte("b"))

	if wm.WindowCount() != 1 {
		t.Errorf("WindowCount = %d, want 1 (same session)", wm.WindowCount())
	}

	// Event after gap — new session
	wm.AddEvent("k1", int64(20*time.Second), []byte("c"))

	if wm.WindowCount() != 2 {
		t.Errorf("WindowCount = %d, want 2 (new session)", wm.WindowCount())
	}

	// Close emits all
	wm.Close()
	if emitted.Load() != 2 {
		t.Errorf("emitted = %d, want 2", emitted.Load())
	}
}

func TestHoppingWindow(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type:  WindowHopping,
		Size:  10 * time.Second,
		Slide: 3 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	wm.AddEvent("k1", int64(2*time.Second), []byte("a"))
	wm.AddEvent("k1", int64(5*time.Second), []byte("b"))

	// Tick to expire
	wm.Tick(int64(15 * time.Second))
	if emitted.Load() == 0 {
		t.Error("should have emitted at least one window")
	}
}

func TestWindowStateAccumulation(t *testing.T) {
	var result *WindowState
	cfg := WindowConfig{
		Type: WindowTumbling,
		Size: 10 * time.Second,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		result = state
	})

	wm.AddEvent("k1", int64(1*time.Second), []byte("hello"))
	wm.AddEvent("k1", int64(3*time.Second), []byte(" world"))

	wm.Tick(int64(11 * time.Second))

	if result == nil {
		t.Fatal("expected state")
	}
	if result.Count != 2 {
		t.Errorf("Count = %d, want 2", result.Count)
	}
	if result.Key != "k1" {
		t.Errorf("Key = %q, want %q", result.Key, "k1")
	}
	if string(result.Data) != "hello world" {
		t.Errorf("Data = %q, want %q", result.Data, "hello world")
	}
}

func TestWindowCloseEmitsAll(t *testing.T) {
	var emitted atomic.Int32
	cfg := WindowConfig{
		Type: WindowTumbling,
		Size: 1 * time.Hour,
	}
	wm := NewWindowManager(cfg, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	wm.AddEvent("k1", int64(time.Second), []byte("a"))
	wm.AddEvent("k2", int64(time.Second), []byte("b"))

	wm.Close()

	if emitted.Load() != 2 {
		t.Errorf("emitted = %d, want 2", emitted.Load())
	}
}
