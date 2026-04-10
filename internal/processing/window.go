package processing

import (
	"sync"
	"time"
)

// WindowType defines the type of window.
type WindowType uint8

const (
	WindowTumbling WindowType = 0 // Fixed, non-overlapping
	WindowSliding  WindowType = 1 // Fixed, overlapping
	WindowSession  WindowType = 2 // Gap-based, dynamic
	WindowHopping  WindowType = 3 // Fixed size, custom hop
)

// WindowConfig configures a window.
type WindowConfig struct {
	Type  WindowType
	Size  time.Duration // window duration
	Slide time.Duration // slide interval (Sliding/Hopping)
	Gap   time.Duration // inactivity gap (Session)
}

// WindowState holds accumulated state for a window key.
type WindowState struct {
	Key       string
	Data      []byte // accumulated state
	Count     int
	LastEvent int64 // nanoseconds
}

// window tracks a single time-based window.
type window struct {
	openTime int64
	closeTime int64
	keys     map[string]*WindowState
}

// WindowManager manages windows for a topology operator.
type WindowManager struct {
	mu      sync.Mutex
	config  WindowConfig
	windows map[int64]*window // open timestamp -> window
	onEmit  func(key string, state *WindowState)
	lastTick int64
}

// NewWindowManager creates a new window manager.
func NewWindowManager(cfg WindowConfig, onEmit func(key string, state *WindowState)) *WindowManager {
	return &WindowManager{
		config:  cfg,
		windows: make(map[int64]*window),
		onEmit:  onEmit,
	}
}

// AddEvent adds an event to the appropriate window(s).
func (wm *WindowManager) AddEvent(key string, timestamp int64, data []byte) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	switch wm.config.Type {
	case WindowTumbling, WindowHopping:
		wm.addToFixedWindow(key, timestamp, data)
	case WindowSliding:
		wm.addToSlidingWindow(key, timestamp, data)
	case WindowSession:
		wm.addToSessionWindow(key, timestamp, data)
	}
}

// Tick checks for expired windows and emits their state.
func (wm *WindowManager) Tick(now int64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.lastTick = now

	for openTime, w := range wm.windows {
		if now >= w.closeTime {
			// Emit all keys in this window
			for k, state := range w.keys {
				if wm.onEmit != nil {
					wm.onEmit(k, state)
				}
			}
			delete(wm.windows, openTime)
		}
	}
}

// WindowCount returns the number of open windows.
func (wm *WindowManager) WindowCount() int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return len(wm.windows)
}

// Close emits all remaining windows.
func (wm *WindowManager) Close() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for _, w := range wm.windows {
		for k, state := range w.keys {
			if wm.onEmit != nil {
				wm.onEmit(k, state)
			}
		}
	}
	wm.windows = nil
}

func (wm *WindowManager) addToFixedWindow(key string, timestamp int64, data []byte) {
	// Calculate which window this event belongs to
	sizeNs := wm.config.Size.Nanoseconds()
	openTime := (timestamp / sizeNs) * sizeNs
	closeTime := openTime + sizeNs

	w, ok := wm.windows[openTime]
	if !ok {
		w = &window{
			openTime:  openTime,
			closeTime: closeTime,
			keys:      make(map[string]*WindowState),
		}
		wm.windows[openTime] = w
	}

	wm.appendToKey(w, key, data, timestamp)
}

func (wm *WindowManager) addToSlidingWindow(key string, timestamp int64, data []byte) {
	sizeNs := wm.config.Size.Nanoseconds()
	slideNs := wm.config.Slide.Nanoseconds()
	if slideNs == 0 {
		slideNs = sizeNs
	}

	// Find the earliest window start that contains this timestamp.
	// A window [start, start+size) contains timestamp if start <= timestamp < start+size.
	// Walk backward by slide intervals to find the earliest valid start.
	start := (timestamp / slideNs) * slideNs
	for start > 0 && start-slideNs+sizeNs > timestamp && start-slideNs <= timestamp {
		start -= slideNs
	}

	// Add to all overlapping windows
	for openTime := start; openTime <= timestamp; openTime += slideNs {
		closeTime := openTime + sizeNs
		if timestamp >= openTime && timestamp < closeTime {
			w, ok := wm.windows[openTime]
			if !ok {
				w = &window{
					openTime:  openTime,
					closeTime: closeTime,
					keys:      make(map[string]*WindowState),
				}
				wm.windows[openTime] = w
			}
			wm.appendToKey(w, key, data, timestamp)
		}
	}
}

func (wm *WindowManager) addToSessionWindow(key string, timestamp int64, data []byte) {
	gapNs := wm.config.Gap.Nanoseconds()

	// Find existing session for this key
	for _, w := range wm.windows {
		state, ok := w.keys[key]
		if !ok {
			continue
		}

		// Check if event falls within the gap
		if timestamp >= w.openTime-gapNs && timestamp <= w.closeTime+gapNs {
			wm.appendToKey(w, key, data, timestamp)

			// Extend window bounds
			if timestamp < w.openTime {
				w.openTime = timestamp
			}
			if timestamp >= w.closeTime {
				w.closeTime = timestamp + 1
			}
			state.LastEvent = timestamp
			return
		}
	}

	// New session window
	w := &window{
		openTime:  timestamp,
		closeTime: timestamp + 1,
		keys:      make(map[string]*WindowState),
	}
	wm.windows[timestamp] = w
	wm.appendToKey(w, key, data, timestamp)
}

func (wm *WindowManager) appendToKey(w *window, key string, data []byte, timestamp int64) {
	state, ok := w.keys[key]
	if !ok {
		state = &WindowState{
			Key:       key,
			LastEvent: timestamp,
		}
		w.keys[key] = state
	}

	if len(data) > 0 {
		state.Data = append(state.Data, data...)
	}
	state.Count++
	if timestamp > state.LastEvent {
		state.LastEvent = timestamp
	}
}
