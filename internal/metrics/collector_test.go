package metrics

import (
	"strings"
	"testing"
)

func TestCollectorCounter(t *testing.T) {
	c := NewCollector()
	c.IncrCounter("test_counter", map[string]string{"key": "val"}, 5)
	c.IncrCounter("test_counter", map[string]string{"key": "val"}, 3)

	output := c.Expose()
	if output == "" {
		t.Error("expected non-empty output")
	}
	// Should contain the counter name
	if len(output) < 10 {
		t.Errorf("output too short: %s", output)
	}
}

func TestCollectorGauge(t *testing.T) {
	c := NewCollector()
	c.SetGauge("test_gauge", map[string]string{"host": "local"}, 42.5)

	output := c.Expose()
	if output == "" {
		t.Error("expected non-empty output")
	}
}

func TestCollectorMessageIn(t *testing.T) {
	c := NewCollector()
	c.MessageIn("orders", 0, "http")
	c.MessageIn("orders", 0, "http")

	output := c.Expose()
	if output == "" {
		t.Error("expected metrics output")
	}
}

func TestCollectorExposeEmpty(t *testing.T) {
	c := NewCollector()
	output := c.Expose()
	if output != "" {
		t.Errorf("expected empty output for unused collector, got: %s", output)
	}
}

func TestCollectorMultipleLabels(t *testing.T) {
	c := NewCollector()
	c.IncrCounter("requests", map[string]string{"method": "GET", "path": "/api"}, 1)
	c.IncrCounter("requests", map[string]string{"method": "POST", "path": "/api"}, 2)

	output := c.Expose()
	if output == "" {
		t.Error("expected output")
	}
}

func TestCollectorMessageOut(t *testing.T) {
	c := NewCollector()
	c.MessageOut("orders", 0, "group-1")
	c.MessageOut("orders", 1, "group-1")

	out := c.Expose()
	if !strings.Contains(out, "chimera_messages_out_total") {
		t.Error("expected messages_out_total in output")
	}
	if !strings.Contains(out, `consumer_group="group-1"`) {
		t.Error("expected consumer_group label")
	}
}

func TestCollectorActiveConnections(t *testing.T) {
	c := NewCollector()
	c.ActiveConnections("http", 5)
	c.ActiveConnections("chimera", 3)

	out := c.Expose()
	if !strings.Contains(out, "chimera_active_connections") {
		t.Error("expected active_connections in output")
	}
	if !strings.Contains(out, `protocol="http"`) {
		t.Error("expected http protocol")
	}
	if !strings.Contains(out, `protocol="chimera"`) {
		t.Error("expected chimera protocol")
	}
}

func TestCollectorQueueDepth(t *testing.T) {
	c := NewCollector()
	c.QueueDepth("task-queue", 42)

	out := c.Expose()
	if !strings.Contains(out, "chimera_queue_depth") {
		t.Error("expected queue_depth in output")
	}
	if !strings.Contains(out, `topic="task-queue"`) {
		t.Error("expected topic label")
	}
}

func TestCollectorConsumerLag(t *testing.T) {
	c := NewCollector()
	c.ConsumerLag("orders", 0, "group-1", 1500)

	out := c.Expose()
	if !strings.Contains(out, "chimera_consumer_lag") {
		t.Error("expected consumer_lag in output")
	}
	if !strings.Contains(out, `consumer_group="group-1"`) {
		t.Error("expected consumer_group label")
	}
}

func TestCollectorGaugeOverwrite(t *testing.T) {
	c := NewCollector()
	c.QueueDepth("topic", 10)
	c.QueueDepth("topic", 20)

	out := c.Expose()
	if strings.Contains(out, " 10") {
		t.Error("gauge should be overwritten, not accumulated")
	}
}

func TestCollectorCounterAccumulates(t *testing.T) {
	c := NewCollector()
	c.MessageIn("t", 0, "http")
	c.MessageIn("t", 0, "http")
	c.MessageIn("t", 0, "http")

	out := c.Expose()
	if !strings.Contains(out, " 3") {
		t.Error("counter should accumulate to 3")
	}
}

func TestCollectorExposeFormat(t *testing.T) {
	c := NewCollector()
	c.IncrCounter("test_counter", map[string]string{"foo": "bar"}, 7)
	c.SetGauge("test_gauge", map[string]string{"baz": "qux"}, 3.14)

	out := c.Expose()
	if !strings.Contains(out, "# TYPE test_counter counter\n") {
		t.Error("expected counter type declaration")
	}
	if !strings.Contains(out, "# TYPE test_gauge gauge\n") {
		t.Error("expected gauge type declaration")
	}
}

func TestCollectorNoLabels(t *testing.T) {
	c := NewCollector()
	c.IncrCounter("bare_counter", nil, 5)

	out := c.Expose()
	if !strings.Contains(out, "bare_counter 5\n") {
		t.Errorf("expected bare_counter without labels, got:\n%s", out)
	}
}

func TestLabelsToKeyOrdering(t *testing.T) {
	c := NewCollector()
	c.IncrCounter("ordered", map[string]string{"z": "1", "a": "2"}, 1)

	out := c.Expose()
	if !strings.Contains(out, `a="2",z="1"`) {
		t.Errorf("expected ordered labels, got:\n%s", out)
	}
}
