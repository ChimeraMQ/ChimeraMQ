package gossip

import (
	"testing"
	"time"
)

func TestHMACTransportCloseAndLocalAddr(t *testing.T) {
	udp, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	ht := NewHMACTransport(udp, []byte("secret"))

	addr := ht.LocalAddr()
	if addr == "" {
		t.Error("expected non-empty local address")
	}

	if err := ht.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestHMACTransportRemoveKey(t *testing.T) {
	udp, _ := NewUDPTransport("127.0.0.1:0")
	defer udp.Close()

	ht := NewHMACTransport(udp, []byte("secret1"))
	ht.AddKey([]byte("secret2"))

	if len(ht.keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(ht.keys))
	}

	ht.RemoveKey(0)
	if len(ht.keys) != 1 {
		t.Errorf("expected 1 key after remove, got %d", len(ht.keys))
	}

	// Remove out of bounds should be no-op
	ht.RemoveKey(-1)
	ht.RemoveKey(99)
	if len(ht.keys) != 1 {
		t.Errorf("expected 1 key after invalid removes, got %d", len(ht.keys))
	}
}

func TestUDPTransportCloseNilConn(t *testing.T) {
	ut := &UDPTransport{conn: nil}
	if err := ut.Close(); err != nil {
		t.Errorf("Close with nil conn should return nil, got %v", err)
	}
}

func TestUDPTransportLocalAddrNilConn(t *testing.T) {
	ut := &UDPTransport{conn: nil}
	if addr := ut.LocalAddr(); addr != "" {
		t.Errorf("LocalAddr with nil conn should return empty, got %q", addr)
	}
}

func TestNewUDPTransportInvalidAddr(t *testing.T) {
	_, err := NewUDPTransport("not-a-valid-address::99999")
	if err == nil {
		t.Error("expected error for invalid UDP address")
	}
}

func TestPhiAccrualDetectorEdgeCases(t *testing.T) {
	d := NewPhiAccrualDetector()

	// Unknown node
	if phi := d.Phi("unknown"); phi != 0 {
		t.Errorf("Phi for unknown node = %f, want 0", phi)
	}

	// Single heartbeat: not enough data
	d.RecordHeartbeat("node-1")
	if phi := d.Phi("node-1"); phi != 0 {
		t.Errorf("Phi after single heartbeat = %f, want 0", phi)
	}

	// Wait > 5s and check threshold path
	time.Sleep(5100 * time.Millisecond)
	phi := d.Phi("node-1")
	if phi <= d.phiThreshold {
		t.Errorf("Phi after long silence = %f, want > %f", phi, d.phiThreshold)
	}
}

func TestPhiAccrualDetectorWithSamples(t *testing.T) {
	d := NewPhiAccrualDetector()

	now := time.Now()
	// Manually create window with multiple arrivals and zero mean edge case
	d.mu.Lock()
	w := &arrivalWindow{
		arrivals:    []time.Duration{500 * time.Millisecond, 600 * time.Millisecond},
		lastArrival: now.Add(-100 * time.Millisecond),
		mean:        550 * time.Millisecond,
		stdDev:      50 * time.Millisecond,
	}
	d.windows["node-2"] = w
	d.mu.Unlock()

	phi := d.Phi("node-2")
	if phi < 0 {
		t.Errorf("Phi = %f, want >= 0", phi)
	}
}

func TestPhiAccrualDetectorMeanZero(t *testing.T) {
	d := NewPhiAccrualDetector()

	d.mu.Lock()
	w := &arrivalWindow{
		arrivals:    []time.Duration{1 * time.Second, 2 * time.Second},
		lastArrival: time.Now().Add(-3 * time.Second),
		mean:        0,
		stdDev:      100 * time.Millisecond,
	}
	d.windows["node-3"] = w
	d.mu.Unlock()

	phi := d.Phi("node-3")
	if phi < 0 {
		t.Errorf("Phi with zero mean = %f, want >= 0", phi)
	}
}

func TestPhiAccrualDetectorStdDevBelowMin(t *testing.T) {
	d := NewPhiAccrualDetector()

	d.mu.Lock()
	w := &arrivalWindow{
		arrivals:    []time.Duration{1 * time.Second, 1*time.Second + 1*time.Millisecond},
		lastArrival: time.Now().Add(-2 * time.Second),
		mean:        1 * time.Second,
		stdDev:      1 * time.Millisecond, // below minStdDev (100ms)
	}
	d.windows["node-4"] = w
	d.mu.Unlock()

	phi := d.Phi("node-4")
	if phi < 0 {
		t.Errorf("Phi with tiny stddev = %f, want >= 0", phi)
	}
}

func TestGenerateHMACKeySuccess(t *testing.T) {
	key, err := GenerateHMACKey()
	if err != nil {
		t.Fatalf("GenerateHMACKey: %v", err)
	}
	if len(key) != 32 {
		t.Errorf("key length = %d, want 32", len(key))
	}
}
