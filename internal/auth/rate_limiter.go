package auth

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// AuthRateLimiter tracks authentication attempts per IP and enforces limits.
type AuthRateLimiter struct {
	mu          sync.Mutex
	attempts    map[string][]time.Time // IP → timestamps of failed attempts
	maxAttempts int                    // max failed attempts per window
	window      time.Duration          // sliding window duration
	banDuration time.Duration          // how long to ban after exceeding maxAttempts
	banned      map[string]time.Time   // IP → ban expiry

	// Trusted proxy configuration for X-Forwarded-For resolution
	trustedProxies []*net.IPNet // CIDR ranges of trusted proxies
}

// RateLimiter is an alias for AuthRateLimiter for external use.
type RateLimiter = AuthRateLimiter

// NewAuthRateLimiter creates a new rate limiter for authentication attempts.
// maxAttempts: maximum failed attempts allowed within the window.
// window: sliding window duration (e.g., 15 minutes).
// banDuration: how long to ban an IP after exceeding the limit (e.g., 30 minutes).
func NewAuthRateLimiter(maxAttempts int, window, banDuration time.Duration) *AuthRateLimiter {
	return &AuthRateLimiter{
		attempts:    make(map[string][]time.Time),
		maxAttempts: maxAttempts,
		window:      window,
		banDuration: banDuration,
		banned:      make(map[string]time.Time),
	}
}

// IsAllowed checks if authentication attempts from the given address are allowed.
// Returns false if the IP is currently banned or has exceeded the rate limit.
func (r *AuthRateLimiter) IsAllowed(addr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if banned
	if until, ok := r.banned[addr]; ok {
		if time.Now().Before(until) {
			return false
		}
		// Ban expired, clean up
		delete(r.banned, addr)
		delete(r.attempts, addr)
		return true
	}

	// Clean old attempts outside the window
	cutoff := time.Now().Add(-r.window)
	attempts := r.attempts[addr]
	valid := 0
	for _, t := range attempts {
		if t.After(cutoff) {
			attempts[valid] = t
			valid++
		}
	}
	r.attempts[addr] = attempts[:valid]

	return len(r.attempts[addr]) < r.maxAttempts
}

// RecordFailed records a failed authentication attempt.
// If the max attempts are exceeded, the IP is banned.
func (r *AuthRateLimiter) RecordFailed(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.attempts[addr] = append(r.attempts[addr], time.Now())

	if len(r.attempts[addr]) > r.maxAttempts {
		r.banned[addr] = time.Now().Add(r.banDuration)
	}
}

// RecordSuccess clears failed attempts for a successful authentication.
func (r *AuthRateLimiter) RecordSuccess(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.attempts, addr)
	delete(r.banned, addr)
}

// ExtractIP extracts the IP address from a network connection's remote address.
func ExtractIP(conn net.Conn) string {
	if conn == nil {
		return "unknown"
	}
	addr := conn.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// SetTrustedProxies configures CIDR ranges for trusted reverse proxies.
// When set, ResolveIP will extract the real client IP from X-Forwarded-For
// or X-Real-IP headers instead of using the direct connection IP.
func (r *AuthRateLimiter) SetTrustedProxies(cidrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.trustedProxies = nil
	for _, cidr := range cidrs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err == nil {
			r.trustedProxies = append(r.trustedProxies, ipNet)
		}
	}
}

// ResolveIP extracts the client IP, respecting X-Forwarded-For/X-Real-IP
// headers when the direct connection IP is from a trusted proxy.
// For direct connections (no proxy), falls back to RemoteAddr.
func (r *AuthRateLimiter) ResolveIP(remoteAddr string, xForwardedFor string, xRealIP string) string {
	// First, check if the direct connection is from a trusted proxy
	r.mu.Lock()
	trusted := len(r.trustedProxies) == 0 // if no proxies configured, trust nothing
	if !trusted {
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			host = remoteAddr
		}
		ip := net.ParseIP(host)
		for _, cidr := range r.trustedProxies {
			if cidr.Contains(ip) {
				trusted = true
				break
			}
		}
	}
	r.mu.Unlock()

	if !trusted {
		// Not from a trusted proxy — use the direct IP
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			return remoteAddr
		}
		return host
	}

	// From a trusted proxy — extract real IP from headers
	// X-Forwarded-For: reject if multiple IPs present (spoofing indicator)
	if xff := strings.TrimSpace(xForwardedFor); xff != "" {
		if !strings.Contains(xff, ",") {
			return xff
		}
		// Multiple IPs — spoofing attempt, fall back to direct IP
	}

	// Fallback to X-Real-IP
	if xri := strings.TrimSpace(xRealIP); xri != "" {
		return xri
	}

	// No header found — fall back to direct IP
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// ResolveIPFromHTTP extracts the client IP from an HTTP request,
// respecting trusted proxy configuration.
func (r *AuthRateLimiter) ResolveIPFromHTTP(req *http.Request) string {
	return r.ResolveIP(req.RemoteAddr, req.Header.Get("X-Forwarded-For"), req.Header.Get("X-Real-IP"))
}
