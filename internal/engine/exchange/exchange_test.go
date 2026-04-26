package exchange

import (
	"testing"
)

func TestDirectExchange(t *testing.T) {
	ex := NewExchange("test-direct", TypeDirect, false)

	ex.Bind("info", "log-info", nil)
	ex.Bind("error", "log-error", nil)
	ex.Bind("info", "log-info-dup", nil) // multiple bindings for same key

	dests := ex.Route("info", nil)
	if len(dests) != 2 {
		t.Fatalf("expected 2 destinations for 'info', got %d: %v", len(dests), dests)
	}

	dests = ex.Route("error", nil)
	if len(dests) != 1 || dests[0] != "log-error" {
		t.Errorf("expected [log-error], got %v", dests)
	}

	dests = ex.Route("warn", nil)
	if len(dests) != 0 {
		t.Errorf("expected no destinations for 'warn', got %v", dests)
	}
}

func TestDirectExchangeUnbind(t *testing.T) {
	ex := NewExchange("unbind", TypeDirect, false)
	ex.Bind("key1", "dest1", nil)
	ex.Bind("key1", "dest2", nil)

	dests := ex.Route("key1", nil)
	if len(dests) != 2 {
		t.Fatalf("expected 2, got %d", len(dests))
	}

	ex.Unbind("key1", "dest1")
	dests = ex.Route("key1", nil)
	if len(dests) != 1 || dests[0] != "dest2" {
		t.Errorf("expected [dest2], got %v", dests)
	}
}

func TestTopicExchange(t *testing.T) {
	ex := NewExchange("test-topic", TypeTopic, false)

	ex.Bind("sensor.*", "sensor-wildcard", nil)
	ex.Bind("sensor.temperature.#", "sensor-temp-all", nil)
	ex.Bind("sensor.temperature.indoor", "sensor-indoor", nil)
	ex.Bind("#", "catch-all", nil)

	tests := []struct {
		key      string
		expected int
		has      []string // must contain these destinations
	}{
		{"sensor.humidity", 2, []string{"sensor-wildcard", "catch-all"}},
		{"sensor.temperature", 3, []string{"sensor-wildcard", "sensor-temp-all", "catch-all"}},
		{"sensor.temperature.indoor", 3, []string{"sensor-temp-all", "sensor-indoor", "catch-all"}},
		{"sensor.temperature.indoor.room1", 2, []string{"sensor-temp-all", "catch-all"}},
		{"alerts.critical", 1, []string{"catch-all"}},
		{"", 1, []string{"catch-all"}},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			dests := ex.Route(tt.key, nil)
			if len(dests) != tt.expected {
				t.Errorf("key=%q: expected %d destinations, got %d: %v", tt.key, tt.expected, len(dests), dests)
				return
			}
			for _, h := range tt.has {
				found := false
				for _, d := range dests {
					if d == h {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("key=%q: missing destination %q in %v", tt.key, h, dests)
				}
			}
		})
	}
}

func TestFanoutExchange(t *testing.T) {
	ex := NewExchange("test-fanout", TypeFanout, false)

	ex.Bind("", "queue1", nil)
	ex.Bind("", "queue2", nil)
	ex.Bind("", "queue3", nil)

	dests := ex.Route("anything", nil)
	if len(dests) != 3 {
		t.Fatalf("expected 3 destinations, got %d", len(dests))
	}

	// Fanout ignores routing key
	dests2 := ex.Route("", nil)
	if len(dests2) != 3 {
		t.Errorf("expected 3 destinations with empty key, got %d", len(dests2))
	}
}

func TestHeadersExchangeAll(t *testing.T) {
	ex := NewExchange("test-headers", TypeHeaders, false)

	ex.Bind("", "dest1", map[string]string{"x-match": "all", "format": "json", "type": "report"})
	ex.Bind("", "dest2", map[string]string{"x-match": "all", "format": "xml"})

	tests := []struct {
		name     string
		headers  map[string]string
		expected []string
	}{
		{
			"match-both-dest1",
			map[string]string{"format": "json", "type": "report"},
			[]string{"dest1"},
		},
		{
			"match-dest2-only",
			map[string]string{"format": "xml"},
			[]string{"dest2"},
		},
		{
			"match-none",
			map[string]string{"format": "csv"},
			nil,
		},
		{
			"partial-dest1-missing-type",
			map[string]string{"format": "json"},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dests := ex.Route("", tt.headers)
			if len(dests) != len(tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, dests)
				return
			}
			for _, exp := range tt.expected {
				found := false
				for _, d := range dests {
					if d == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("missing %q in %v", exp, dests)
				}
			}
		})
	}
}

func TestHeadersExchangeAny(t *testing.T) {
	ex := NewExchange("test-any", TypeHeaders, false)

	ex.Bind("", "dest1", map[string]string{"x-match": "any", "format": "json", "priority": "high"})

	// Match one of the headers
	dests := ex.Route("", map[string]string{"format": "json"})
	if len(dests) != 1 || dests[0] != "dest1" {
		t.Errorf("expected [dest1], got %v", dests)
	}

	// Match the other header
	dests = ex.Route("", map[string]string{"priority": "high"})
	if len(dests) != 1 || dests[0] != "dest1" {
		t.Errorf("expected [dest1], got %v", dests)
	}

	// No match
	dests = ex.Route("", map[string]string{"format": "xml"})
	if len(dests) != 0 {
		t.Errorf("expected no match, got %v", dests)
	}
}

func TestTopicMatchExact(t *testing.T) {
	if !topicMatch("sensor.temp", "sensor.temp") {
		t.Error("exact match should work")
	}
	if topicMatch("sensor.temp", "sensor.humidity") {
		t.Error("non-matching keys should not match")
	}
}

func TestTopicMatchWildcard(t *testing.T) {
	if !topicMatch("sensor.*", "sensor.temp") {
		t.Error("* should match single word")
	}
	if topicMatch("sensor.*", "sensor.temp.indoor") {
		t.Error("* should not match multiple words")
	}
}

func TestTopicMatchHash(t *testing.T) {
	if !topicMatch("sensor.#", "sensor") {
		t.Error("# should match zero words")
	}
	if !topicMatch("sensor.#", "sensor.temp") {
		t.Error("# should match one word")
	}
	if !topicMatch("sensor.#", "sensor.temp.indoor.room1") {
		t.Error("# should match multiple words")
	}
	if !topicMatch("#", "anything.at.all") {
		t.Error("lone # should match everything")
	}
}

func TestTopicMatchComplexPatterns(t *testing.T) {
	tests := []struct {
		pattern string
		key     string
		match   bool
	}{
		{"*.*.info", "app.server.info", true},
		{"*.*.info", "app.server.warn", false},
		{"*.server.#", "app.server", true},
		{"*.server.#", "app.server.info", true},
		{"*.server.#", "app.server.info.error", true},
		{"*.server.#", "app.client.info", false},
		{"#.#", "a.b.c", true}, // degenerate case
		{"sensor.*.temp.#", "sensor.indoor.temp.room1", true},
		{"sensor.*.temp.#", "sensor.temp", false}, // * requires one word
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"|"+tt.key, func(t *testing.T) {
			got := topicMatch(tt.pattern, tt.key)
			if got != tt.match {
				t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.pattern, tt.key, got, tt.match)
			}
		})
	}
}

func TestExchangeTypeFromString(t *testing.T) {
	tests := []struct {
		input string
		want  ExchangeType
	}{
		{"direct", TypeDirect},
		{"topic", TypeTopic},
		{"fanout", TypeFanout},
		{"headers", TypeHeaders},
		{"Direct", TypeDirect},
		{"unknown", TypeDirect},
	}
	for _, tt := range tests {
		got := ExchangeTypeFromString(tt.input)
		if got != tt.want {
			t.Errorf("ExchangeTypeFromString(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestExchangeTypeString(t *testing.T) {
	if TypeDirect.String() != "direct" {
		t.Errorf("TypeDirect.String() = %q", TypeDirect.String())
	}
	if TypeTopic.String() != "topic" {
		t.Errorf("TypeTopic.String() = %q", TypeTopic.String())
	}
	if TypeFanout.String() != "fanout" {
		t.Errorf("TypeFanout.String() = %q", TypeFanout.String())
	}
	if TypeHeaders.String() != "headers" {
		t.Errorf("TypeHeaders.String() = %q", TypeHeaders.String())
	}
	// Unknown type
	unknown := ExchangeType(99)
	if unknown.String() != "unknown" {
		t.Errorf("unknown ExchangeType.String() = %q, want unknown", unknown.String())
	}
}

func TestExchangeName(t *testing.T) {
	ex := NewExchange("my-exchange", TypeDirect, true)
	if ex.Name() != "my-exchange" {
		t.Errorf("Name() = %q, want my-exchange", ex.Name())
	}
}

func TestExchangeBindings(t *testing.T) {
	ex := NewExchange("test", TypeDirect, false)
	ex.Bind("k1", "d1", nil)
	ex.Bind("k2", "d2", nil)

	bindings := ex.Bindings()
	if len(bindings) != 2 {
		t.Fatalf("expected 2 bindings, got %d", len(bindings))
	}

	// Returned slice should be a copy — modifying it shouldn't affect the exchange
	bindings[0] = Binding{Key: "modified", Destination: "modified"}
	original := ex.Bindings()
	if original[0].Key == "modified" {
		t.Error("Bindings() should return a copy")
	}
}

func TestHeadersMatchEmpty(t *testing.T) {
	// Empty binding args matches everything
	if !headersMatch(map[string]string{}, map[string]string{"a": "b"}) {
		t.Error("empty binding args should match")
	}
	// Empty msg headers doesn't match non-empty binding args
	if headersMatch(map[string]string{"a": "b"}, map[string]string{}) {
		t.Error("non-empty binding args should not match empty headers")
	}
}
