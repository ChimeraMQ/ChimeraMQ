package ttl

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

func TestIsExpired(t *testing.T) {
	env := &message.Envelope{
		Timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
		TTL:       int64(5 * time.Second),
	}
	if !IsExpired(env) {
		t.Error("should be expired")
	}
}

func TestIsNotExpired(t *testing.T) {
	env := &message.Envelope{
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(5 * time.Second),
	}
	if IsExpired(env) {
		t.Error("should not be expired")
	}
}

func TestIsExpiredZeroTTL(t *testing.T) {
	env := &message.Envelope{
		Timestamp: time.Now().Add(-100 * time.Hour).UnixNano(),
		TTL:       0,
	}
	if IsExpired(env) {
		t.Error("zero TTL should never expire")
	}
}

func TestApplyDefaultTTL(t *testing.T) {
	env := &message.Envelope{}

	ApplyDefaultTTL(env, int64(30*time.Second))
	if env.TTL != int64(30*time.Second) {
		t.Errorf("TTL = %d, want %d", env.TTL, int64(30*time.Second))
	}

	// Should not override existing TTL
	env.TTL = int64(10 * time.Second)
	ApplyDefaultTTL(env, int64(30*time.Second))
	if env.TTL != int64(10*time.Second) {
		t.Error("should not override existing TTL")
	}

	// Zero default should not set TTL
	env2 := &message.Envelope{}
	ApplyDefaultTTL(env2, 0)
	if env2.TTL != 0 {
		t.Error("zero default should not set TTL")
	}
}

func TestExpirerStartStop(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	e := NewExpirer(storage)
	e.Start()
	time.Sleep(50 * time.Millisecond)
	e.Stop()
}

func TestExpirerSetRemoveTopic(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	e := NewExpirer(storage)
	e.SetTopicConfig("test", &TopicTTLConfig{
		DefaultTTL: int64(5 * time.Second),
		Action:     ActionDrop,
	})

	if len(e.configs) != 1 {
		t.Errorf("configs count = %d, want 1", len(e.configs))
	}

	e.RemoveTopic("test")
	if len(e.configs) != 0 {
		t.Errorf("configs count after remove = %d, want 0", len(e.configs))
	}
}
