package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Collector provides Prometheus-compatible metrics collection.
type Collector struct {
	mu       sync.Mutex
	counters map[string]*Counter
	gauges   map[string]*Gauge
}

// Counter tracks a monotonically increasing value.
type Counter struct {
	mu     sync.Mutex
	values map[string]uint64
}

// Gauge tracks a value that can go up or down.
type Gauge struct {
	mu     sync.Mutex
	values map[string]float64
}

// NewCollector creates a new metrics collector.
func NewCollector() *Collector {
	return &Collector{
		counters: make(map[string]*Counter),
		gauges:   make(map[string]*Gauge),
	}
}

// IncrCounter increments a counter by delta.
func (c *Collector) IncrCounter(name string, labels map[string]string, delta uint64) {
	c.mu.Lock()
	counter, ok := c.counters[name]
	if !ok {
		counter = &Counter{values: make(map[string]uint64)}
		c.counters[name] = counter
	}
	c.mu.Unlock()

	key := labelsToKey(labels)
	counter.mu.Lock()
	counter.values[key] += delta
	counter.mu.Unlock()
}

// SetGauge sets a gauge to a value.
func (c *Collector) SetGauge(name string, labels map[string]string, value float64) {
	c.mu.Lock()
	gauge, ok := c.gauges[name]
	if !ok {
		gauge = &Gauge{values: make(map[string]float64)}
		c.gauges[name] = gauge
	}
	c.mu.Unlock()

	key := labelsToKey(labels)
	gauge.mu.Lock()
	gauge.values[key] = value
	gauge.mu.Unlock()
}

// Expose returns Prometheus text format exposition.
func (c *Collector) Expose() string {
	var buf strings.Builder

	c.mu.Lock()
	// Snapshot under single lock to avoid concurrent modification
	counters := make(map[string]*Counter, len(c.counters))
	for k, v := range c.counters {
		counters[k] = v
	}
	gauges := make(map[string]*Gauge, len(c.gauges))
	for k, v := range c.gauges {
		gauges[k] = v
	}
	c.mu.Unlock()

	for name, counter := range counters {
		buf.WriteString(fmt.Sprintf("# TYPE %s counter\n", name))
		counter.mu.Lock()
		for labels, value := range counter.values {
			if labels == "" {
				buf.WriteString(fmt.Sprintf("%s %d\n", name, value))
			} else {
				buf.WriteString(fmt.Sprintf("%s{%s} %d\n", name, labels, value))
			}
		}
		counter.mu.Unlock()
	}

	for name, gauge := range gauges {
		buf.WriteString(fmt.Sprintf("# TYPE %s gauge\n", name))
		gauge.mu.Lock()
		for labels, value := range gauge.values {
			if labels == "" {
				buf.WriteString(fmt.Sprintf("%s %g\n", name, value))
			} else {
				buf.WriteString(fmt.Sprintf("%s{%s} %g\n", name, labels, value))
			}
		}
		gauge.mu.Unlock()
	}

	return buf.String()
}

// MessageIn increments the messages-in counter.
func (c *Collector) MessageIn(topic string, partition uint32, proto string) {
	c.IncrCounter("chimera_messages_in_total", map[string]string{
		"topic":     topic,
		"partition": fmt.Sprintf("%d", partition),
		"protocol":  proto,
	}, 1)
}

// MessageOut increments the messages-out counter.
func (c *Collector) MessageOut(topic string, partition uint32, group string) {
	c.IncrCounter("chimera_messages_out_total", map[string]string{
		"topic":          topic,
		"partition":      fmt.Sprintf("%d", partition),
		"consumer_group": group,
	}, 1)
}

// ActiveConnections sets the active connections gauge.
func (c *Collector) ActiveConnections(proto string, count int) {
	c.SetGauge("chimera_active_connections", map[string]string{
		"protocol": proto,
	}, float64(count))
}

// QueueDepth sets the queue depth gauge.
func (c *Collector) QueueDepth(topic string, depth int) {
	c.SetGauge("chimera_queue_depth", map[string]string{
		"topic": topic,
	}, float64(depth))
}

// ConsumerLag sets the consumer lag gauge.
func (c *Collector) ConsumerLag(topic string, partition uint32, group string, lag uint64) {
	c.SetGauge("chimera_consumer_lag", map[string]string{
		"topic":          topic,
		"partition":      fmt.Sprintf("%d", partition),
		"consumer_group": group,
	}, float64(lag))
}

func labelsToKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf strings.Builder
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(k)
		buf.WriteString(`="`)
		buf.WriteString(labels[k])
		buf.WriteByte('"')
	}
	return buf.String()
}
