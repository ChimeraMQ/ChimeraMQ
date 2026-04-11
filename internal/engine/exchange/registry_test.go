package exchange

import (
	"testing"
)

func TestRegistryDeclare(t *testing.T) {
	r := NewRegistry()

	ex, err := r.Declare("events", TypeTopic, true)
	if err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if ex.Name() != "events" {
		t.Errorf("name = %q, want events", ex.Name())
	}

	// Re-declare with same type should return existing
	ex2, err := r.Declare("events", TypeTopic, true)
	if err != nil {
		t.Fatalf("re-declare: %v", err)
	}
	if ex2 != ex {
		t.Error("re-declare should return same exchange")
	}

	// Re-declare with different type should fail
	_, err = r.Declare("events", TypeDirect, false)
	if err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestRegistryDelete(t *testing.T) {
	r := NewRegistry()
	r.Declare("test", TypeDirect, false)

	if err := r.Delete("test"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok := r.Get("test"); ok {
		t.Error("exchange should be deleted")
	}
}

func TestRegistryDeleteNotFound(t *testing.T) {
	r := NewRegistry()
	err := r.Delete("nonexistent")
	if err == nil {
		t.Error("expected error for missing exchange")
	}
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()
	r.Declare("test", TypeFanout, false)

	ex, ok := r.Get("test")
	if !ok || ex == nil {
		t.Error("expected exchange")
	}

	_, ok = r.Get("missing")
	if ok {
		t.Error("should not find missing exchange")
	}
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()
	r.Declare("a", TypeDirect, false)
	r.Declare("b", TypeTopic, false)
	r.Declare("c", TypeFanout, false)

	list := r.List()
	if len(list) != 3 {
		t.Fatalf("expected 3 exchanges, got %d", len(list))
	}
}

func TestRegistryRoute(t *testing.T) {
	r := NewRegistry()
	ex, _ := r.Declare("events", TypeTopic, false)
	ex.Bind("sensor.#", "sensor-topic", nil)

	dests, err := r.Route("events", "sensor.temp.indoor", nil)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if len(dests) != 1 || dests[0] != "sensor-topic" {
		t.Errorf("expected [sensor-topic], got %v", dests)
	}
}

func TestRegistryRouteNotFound(t *testing.T) {
	r := NewRegistry()
	_, err := r.Route("missing", "key", nil)
	if err == nil {
		t.Error("expected error for missing exchange")
	}
}

func TestRegistryBindUnbind(t *testing.T) {
	r := NewRegistry()
	r.Declare("ex", TypeDirect, false)

	if err := r.Bind("ex", "key1", "dest1", nil); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	dests, _ := r.Route("ex", "key1", nil)
	if len(dests) != 1 {
		t.Errorf("expected 1 destination, got %d", len(dests))
	}

	if err := r.Unbind("ex", "key1", "dest1"); err != nil {
		t.Fatalf("Unbind: %v", err)
	}

	dests, _ = r.Route("ex", "key1", nil)
	if len(dests) != 0 {
		t.Errorf("expected 0 destinations after unbind, got %d", len(dests))
	}
}

func TestRegistryBindNotFound(t *testing.T) {
	r := NewRegistry()
	err := r.Bind("missing", "key", "dest", nil)
	if err == nil {
		t.Error("expected error for missing exchange")
	}
}

func TestRegistryUnbindNotFound(t *testing.T) {
	r := NewRegistry()
	err := r.Unbind("missing", "key", "dest")
	if err == nil {
		t.Error("expected error for missing exchange")
	}
}
