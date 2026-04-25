package ws

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestExtractRealIPTrustedProxy(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:8080"
	req.Header.Set("X-Forwarded-For", "192.168.1.1")

	ip := extractRealIP(req, "10.0.0.0/8")
	if ip != "192.168.1.1" {
		t.Errorf("extractRealIP = %q, want 192.168.1.1", ip)
	}
}

func TestExtractRealIPUntrustedProxy(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:8080"
	req.Header.Set("X-Forwarded-For", "10.0.0.1")

	ip := extractRealIP(req, "10.0.0.0/8")
	if ip != "192.168.1.1" {
		t.Errorf("extractRealIP = %q, want 192.168.1.1", ip)
	}
}

func TestExtractRealIPNoTrustedCIDR(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "127.0.0.1:8080"
	req.Header.Set("X-Forwarded-For", "10.0.0.1")

	ip := extractRealIP(req, "")
	if ip != "127.0.0.1" {
		t.Errorf("extractRealIP = %q, want 127.0.0.1", ip)
	}
}

func TestExtractRealIPNoPort(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "127.0.0.1"

	ip := extractRealIP(req, "")
	if ip != "127.0.0.1" {
		t.Errorf("extractRealIP = %q, want 127.0.0.1", ip)
	}
}

func TestExtractRealIPMultipleXFF(t *testing.T) {
	// Multiple IPs in X-Forwarded-For — should NOT extract from header
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:8080"
	req.Header.Set("X-Forwarded-For", "192.168.1.1, 10.0.0.2")

	ip := extractRealIP(req, "10.0.0.0/8")
	if ip != "10.0.0.1" {
		t.Errorf("extractRealIP = %q, want 10.0.0.1", ip)
	}
}

func TestExtractRealIPInvalidCIDR(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:8080"
	req.Header.Set("X-Forwarded-For", "192.168.1.1")

	ip := extractRealIP(req, "invalid")
	if ip != "10.0.0.1" {
		t.Errorf("extractRealIP = %q, want 10.0.0.1", ip)
	}
}

func TestWsSanitizeError(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"not found", "not found"},
		{"/some/path/file.go:42", "internal error"},
		{"goroutine 123 [running]", "internal error"},
		{"no data available", "internal error"},
	}

	for _, tt := range tests {
		err := fakeErr{msg: tt.input}
		got := wsSanitizeError(err)
		if got != tt.want {
			t.Errorf("wsSanitizeError(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

type fakeErr struct{ msg string }

func (e fakeErr) Error() string { return e.msg }

func TestAllowMessageRateLimit(t *testing.T) {
	sess := &wsSession{
		rateTokens: wsRateBurst,
		rateLast:   time.Now(),
	}

	// Exhaust all tokens
	for i := int64(0); i < wsRateBurst; i++ {
		if !sess.allowMessage() {
			t.Fatalf("message %d should be allowed", i+1)
		}
	}

	// Next should be blocked
	if sess.allowMessage() {
		t.Error("message should be blocked after rate limit exceeded")
	}
}

func TestAllowMessageRefill(t *testing.T) {
	sess := &wsSession{
		rateTokens: 0,
		rateLast:   time.Now().Add(-1 * time.Second), // 1 second ago
	}

	// Should have refilled tokens
	if !sess.allowMessage() {
		t.Error("message should be allowed after token refill")
	}
}
