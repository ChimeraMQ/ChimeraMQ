package queue

import (
	"testing"
	"time"
)

func TestAckTrackerTrackAndAck(t *testing.T) {
	at := NewAckTracker(30 * time.Second)
	defer at.Stop()

	at.Track(1, "c1", 0, 0)
	at.Track(2, "c1", 0, 0)

	if !at.Ack(1) {
		t.Error("expected ack to succeed")
	}
	if at.Ack(1) {
		t.Error("expected second ack to fail")
	}
	if !at.Ack(2) {
		t.Error("expected ack to succeed")
	}
}

func TestAckTrackerNack(t *testing.T) {
	at := NewAckTracker(30 * time.Second)
	defer at.Stop()

	at.Track(1, "c1", 0, 3)

	shouldDLQ, count := at.Nack(1)
	if shouldDLQ {
		t.Error("should not DLQ on first nack")
	}
	if count != 1 {
		t.Errorf("expected deliverCount=1, got %d", count)
	}
}

func TestAckTrackerNackDLQ(t *testing.T) {
	at := NewAckTracker(30 * time.Second)
	defer at.Stop()

	at.Track(1, "c1", 2, 3) // deliverCount=2, maxRetries=3

	shouldDLQ, count := at.Nack(1)
	if !shouldDLQ {
		t.Error("expected DLQ when deliverCount reaches maxRetries")
	}
	if count != 3 {
		t.Errorf("expected deliverCount=3, got %d", count)
	}
}

func TestAckTrackerNackUnknown(t *testing.T) {
	at := NewAckTracker(30 * time.Second)
	defer at.Stop()

	shouldDLQ, count := at.Nack(999)
	if shouldDLQ {
		t.Error("should not DLQ unknown offset")
	}
	if count != 0 {
		t.Errorf("expected count=0, got %d", count)
	}
}
