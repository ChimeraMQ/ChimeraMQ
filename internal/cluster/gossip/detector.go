package gossip

import (
	"math"
	"sync"
	"time"
)

// PhiAccrualDetector implements phi accrual failure detection.
type PhiAccrualDetector struct {
	mu           sync.Mutex
	windows      map[NodeID]*arrivalWindow
	maxSamples   int
	minStdDev    time.Duration
	phiThreshold float64
}

type arrivalWindow struct {
	arrivals   []time.Duration
	lastArrival time.Time
	mean       time.Duration
	stdDev     time.Duration
}

// NewPhiAccrualDetector creates a new phi accrual detector.
func NewPhiAccrualDetector() *PhiAccrualDetector {
	return &PhiAccrualDetector{
		windows:      make(map[NodeID]*arrivalWindow),
		maxSamples:   1000,
		minStdDev:    100 * time.Millisecond,
		phiThreshold: 8.0,
	}
}

// RecordHeartbeat records a heartbeat arrival from a node.
func (d *PhiAccrualDetector) RecordHeartbeat(id NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	w, ok := d.windows[id]
	if !ok {
		w = &arrivalWindow{
			arrivals: make([]time.Duration, 0, d.maxSamples),
		}
		d.windows[id] = w
	}

	if !w.lastArrival.IsZero() {
		interval := now.Sub(w.lastArrival)
		w.arrivals = append(w.arrivals, interval)
		if len(w.arrivals) > d.maxSamples {
			w.arrivals = w.arrivals[1:]
		}
		d.updateStats(w)
	}
	w.lastArrival = now
}

// Phi returns the phi value for a node (higher = more likely failed).
func (d *PhiAccrualDetector) Phi(id NodeID) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	w, ok := d.windows[id]
	if !ok || w.lastArrival.IsZero() {
		return 0.0
	}

	elapsed := time.Since(w.lastArrival)
	if len(w.arrivals) < 2 {
		// Not enough data, use simple threshold
		if elapsed > 5*time.Second {
			return d.phiThreshold + 1
		}
		return 0.0
	}

	stdDev := w.stdDev
	if stdDev < d.minStdDev {
		stdDev = d.minStdDev
	}

	mean := w.mean
	if mean == 0 {
		mean = 1 * time.Second
	}

	// Phi calculation using normal distribution
	// phi = -log10(1 - CDF(elapsed))
	// Approximation: use the ratio of elapsed to mean
	y := (float64(elapsed) - float64(mean)) / float64(stdDev)
	// Normal CDF approximation using error function
	// For large y, phi grows proportionally
	// phi ≈ y / (mean * ln(10)) for the basic case
	phi := y / math.Log(10)
	if phi < 0 {
		phi = 0
	}

	return phi
}

// IsSuspect returns true if the node should be suspected.
func (d *PhiAccrualDetector) IsSuspect(id NodeID) bool {
	return d.Phi(id) > d.phiThreshold
}

// Remove removes a node from tracking.
func (d *PhiAccrualDetector) Remove(id NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.windows, id)
}

func (d *PhiAccrualDetector) updateStats(w *arrivalWindow) {
	if len(w.arrivals) == 0 {
		return
	}
	var sum time.Duration
	for _, a := range w.arrivals {
		sum += a
	}
	w.mean = sum / time.Duration(len(w.arrivals))

	if len(w.arrivals) < 2 {
		w.stdDev = d.minStdDev
		return
	}

	var sqSum float64
	meanF := float64(w.mean)
	for _, a := range w.arrivals {
		diff := float64(a) - meanF
		sqSum += diff * diff
	}
	variance := sqSum / float64(len(w.arrivals)-1)
	w.stdDev = time.Duration(math.Sqrt(variance))
	if w.stdDev < d.minStdDev {
		w.stdDev = d.minStdDev
	}
}
