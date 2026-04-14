package auth

import (
	"net"
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
