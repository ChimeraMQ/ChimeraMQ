package stream

import "testing"

func TestWaiterRegisterNotify(t *testing.T) {
	wr := NewWaiterRegistry()

	ch := wr.Register("topic1", 0)
	wr.Notify("topic1", 0)

	select {
	case <-ch:
		// expected
	default:
		t.Error("expected notification")
	}
}

func TestWaiterNotifyNoWaiters(t *testing.T) {
	wr := NewWaiterRegistry()
	// Should not panic
	wr.Notify("no-topic", 0)
}

func TestWaiterUnregister(t *testing.T) {
	wr := NewWaiterRegistry()
	ch := wr.Register("topic1", 0)
	wr.Unregister("topic1", 0, ch)

	wr.Notify("topic1", 0)
	select {
	case <-ch:
		t.Error("should not receive after unregister")
	default:
		// expected
	}
}

func TestWaiterMultipleWaiters(t *testing.T) {
	wr := NewWaiterRegistry()
	ch1 := wr.Register("topic1", 0)
	ch2 := wr.Register("topic1", 0)

	wr.Notify("topic1", 0)

	// Both should be notified
	select {
	case <-ch1:
	default:
		t.Error("ch1 not notified")
	}
	select {
	case <-ch2:
	default:
		t.Error("ch2 not notified")
	}
}
