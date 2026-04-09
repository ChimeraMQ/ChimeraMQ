package mqtt

import (
	"testing"
)

func TestDetector(t *testing.T) {
	d := &Detector{}

	tests := []struct {
		peek  []byte
		match bool
	}{
		{[]byte{0x10}, true},     // MQTT CONNECT
		{[]byte{0x30}, false},    // PUBLISH (not CONNECT)
		{[]byte{0xC0}, false},    // PINGREQ
		{[]byte{0x00}, false},    // invalid
		{[]byte{'G', 'E', 'T'}, false}, // HTTP
		{[]byte{'C', 'H', 'M', 'R'}, false}, // Chimera
	}
	for _, tt := range tests {
		got := d.Detect(tt.peek)
		if got != tt.match {
			t.Errorf("Detect(%x) = %v, want %v", tt.peek, got, tt.match)
		}
	}
	if d.BytesNeeded() != 1 {
		t.Errorf("BytesNeeded() = %d, want 1", d.BytesNeeded())
	}
}

func TestTopicMapper(t *testing.T) {
	tm := NewTopicMapper(".")

	tests := []struct {
		mqtt   string
		chimera string
	}{
		{"sensor/temp", "sensor.temp"},
		{"a/b/c", "a.b.c"},
		{"single", "single"},
		{"", ""},
	}
	for _, tt := range tests {
		got := tm.MQTTToChimera(tt.mqtt)
		if got != tt.chimera {
			t.Errorf("MQTTToChimera(%q) = %q, want %q", tt.mqtt, got, tt.chimera)
		}
		// Reverse
		back := tm.ChimeraToMQTT(tt.chimera)
		if back != tt.mqtt {
			t.Errorf("ChimeraToMQTT(%q) = %q, want %q", tt.chimera, back, tt.mqtt)
		}
	}
}

func TestFilterMatches(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		match  bool
	}{
		{"sensor/temperature", "sensor/temperature", true},
		{"sensor/temperature", "sensor/humidity", false},
		{"sensor/+", "sensor/temp", true},
		{"sensor/+", "sensor/temp/room", false},
		{"#", "anything/goes", true},
		{"sensor/#", "sensor/temp/room1", true},
		{"sensor/#", "sensor", true},
		{"+/+", "a/b", true},
		{"+/+", "a/b/c", false},
		{"sensor/+/room", "sensor/temp/room", true},
		{"sensor/+/room", "sensor/humidity/room", true},
		{"sensor/+/room", "sensor/temp/kitchen", false},
	}

	for _, tt := range tests {
		got := FilterMatches(tt.filter, tt.topic)
		if got != tt.match {
			t.Errorf("FilterMatches(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.match)
		}
	}
}

func TestRetainedStore(t *testing.T) {
	rs := NewRetainedStore(10)

	// Store a retained message
	rs.Store("sensor/temp", []byte("25.5"), 0)

	// Retrieve
	msgs := rs.Matching("sensor/temp")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 retained, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "25.5" {
		t.Errorf("payload = %q, want 25.5", msgs[0].Payload)
	}

	// Wildcard match
	msgs = rs.Matching("sensor/#")
	if len(msgs) != 1 {
		t.Errorf("expected 1 retained for sensor/#, got %d", len(msgs))
	}

	// Remove by storing empty payload
	rs.Store("sensor/temp", nil, 0)
	msgs = rs.Matching("sensor/temp")
	if len(msgs) != 0 {
		t.Errorf("expected 0 after delete, got %d", len(msgs))
	}
}

func TestRetainedStoreWildcard(t *testing.T) {
	rs := NewRetainedStore(100)

	rs.Store("sensor/temp/room1", []byte("25"), 0)
	rs.Store("sensor/temp/room2", []byte("26"), 0)
	rs.Store("sensor/humidity/room1", []byte("60"), 0)

	msgs := rs.Matching("sensor/temp/+")
	if len(msgs) != 2 {
		t.Errorf("expected 2 for sensor/temp/+, got %d", len(msgs))
	}

	msgs = rs.Matching("#")
	if len(msgs) != 3 {
		t.Errorf("expected 3 for #, got %d", len(msgs))
	}
}

func TestSession(t *testing.T) {
	s := NewSession("client-1", true, 60)

	if s.ClientID() != "client-1" {
		t.Errorf("clientID = %q", s.ClientID())
	}

	// Packet IDs
	pid1 := s.NextPacketID()
	pid2 := s.NextPacketID()
	if pid1 == pid2 {
		t.Errorf("packet IDs should differ: %d == %d", pid1, pid2)
	}

	// Subscriptions
	s.AddSub("topic/a", 0)
	s.AddSub("topic/b", 1)
	subs := s.Subscriptions()
	if len(subs) != 2 {
		t.Errorf("subs = %d, want 2", len(subs))
	}

	s.RemoveSub("topic/a")
	subs = s.Subscriptions()
	if len(subs) != 1 {
		t.Errorf("subs after remove = %d, want 1", len(subs))
	}

	// Will
	s.SetWill("will/topic", []byte("goodbye"), 1, false)
	w := s.TakeWill()
	if w == nil {
		t.Fatal("expected will message")
	}
	if w.topic != "will/topic" {
		t.Errorf("will topic = %q", w.topic)
	}
	// Second take should return nil
	w2 := s.TakeWill()
	if w2 != nil {
		t.Error("will should be cleared after TakeWill")
	}

	// Inflight
	s.AddInflight(pid1, "topic", []byte("msg"), 1)
	s.AckInflight(pid1)
	// No panic = success

	// Keepalive
	if s.KeepAliveDuration() != 60e9 {
		t.Errorf("keepalive = %v, want 60s", s.KeepAliveDuration())
	}
}
