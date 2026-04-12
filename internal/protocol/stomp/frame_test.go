package stomp

import (
	"bufio"
	"bytes"
	"testing"
)

func TestFrameEncode(t *testing.T) {
	frame := NewFrame(CmdConnected)
	frame.Set("version", "1.2")
	frame.Set("session", "test-session")

	data := frame.Encode()
	if len(data) == 0 {
		t.Fatal("encoded frame is empty")
	}

	// Should end with null byte
	if data[len(data)-1] != 0 {
		t.Error("frame should end with null byte")
	}

	// Should contain command
	if !bytes.Contains(data, []byte("CONNECTED")) {
		t.Error("frame should contain CONNECTED command")
	}
}

func TestFrameGetSet(t *testing.T) {
	frame := NewFrame(CmdSend)
	frame.Set("destination", "/topic/test")
	frame.Set("content-type", "text/plain")

	// Test Get with exact case
	if v := frame.Get("destination"); v != "/topic/test" {
		t.Errorf("expected '/topic/test', got '%s'", v)
	}

	// Test Get with different case
	if v := frame.Get("Destination"); v != "/topic/test" {
		t.Errorf("case-insensitive get failed: expected '/topic/test', got '%s'", v)
	}

	// Test Get missing key
	if v := frame.Get("missing"); v != "" {
		t.Errorf("expected empty string for missing key, got '%s'", v)
	}
}

func TestReadFrame(t *testing.T) {
	// Create a simple STOMP frame
	input := "CONNECT\nlogin:user\npasscode:secret\n\n\x00"
	reader := bufio.NewReader(bytes.NewReader([]byte(input)))

	frame, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if frame.Command != CmdConnect {
		t.Errorf("expected command CONNECT, got %s", frame.Command)
	}

	if v := frame.Get("login"); v != "user" {
		t.Errorf("expected login 'user', got '%s'", v)
	}

	if v := frame.Get("passcode"); v != "secret" {
		t.Errorf("expected passcode 'secret', got '%s'", v)
	}
}

func TestReadFrameWithBody(t *testing.T) {
	// Create a STOMP frame with body
	input := "SEND\ndestination:/topic/test\ncontent-length:11\n\nHello World\x00"
	reader := bufio.NewReader(bytes.NewReader([]byte(input)))

	frame, err := ReadFrame(reader)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if frame.Command != CmdSend {
		t.Errorf("expected command SEND, got %s", frame.Command)
	}

	if v := frame.Get("destination"); v != "/topic/test" {
		t.Errorf("expected destination '/topic/test', got '%s'", v)
	}

	if string(frame.Body) != "Hello World" {
		t.Errorf("expected body 'Hello World', got '%s'", string(frame.Body))
	}
}

func TestEncodeDecodeHeader(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with:colon", "with\\ccolon"},
		{"with\\backslash", "with\\\\backslash"},
		{"with\nnewline", "with\\nnewline"},
	}

	for _, tc := range tests {
		encoded := encodeHeader(tc.input)
		if encoded != tc.expected {
			t.Errorf("encodeHeader(%q) = %q, expected %q", tc.input, encoded, tc.expected)
		}

		decoded := decodeHeader(encoded)
		if decoded != tc.input {
			t.Errorf("decodeHeader(%q) = %q, expected %q", encoded, decoded, tc.input)
		}
	}
}

func TestIsClientCommand(t *testing.T) {
	clientCmds := []Command{CmdConnect, CmdStomp, CmdSend, CmdSubscribe, CmdUnsubscribe,
		CmdBegin, CmdCommit, CmdAbort, CmdAck, CmdNack, CmdDisconnect}

	for _, cmd := range clientCmds {
		if !IsClientCommand(cmd) {
			t.Errorf("%s should be a client command", cmd)
		}
	}

	// Server commands should not be client commands
	serverCmds := []Command{CmdConnected, CmdMessage, CmdReceipt, CmdError}
	for _, cmd := range serverCmds {
		if IsClientCommand(cmd) {
			t.Errorf("%s should not be a client command", cmd)
		}
	}
}

func TestDetector(t *testing.T) {
	d := &Detector{}

	tests := []struct {
		peek     string
		expected bool
	}{
		{"CONNECT", true},
		{"STOMP\n", true},
		{"SEND\n", true},
		{"SUBS", true},
		{"UNSUB", true},
		{"BEGIN", true},
		{"COMMIT", true},
		{"ABORT", true},
		{"ACK ", true},
		{"NACK", true},
		{"DISC", true},
		{"HTTP/", false},
		{"GET /", false},
		{"AMQP", false},
		{"MQTT", false},
	}

	for _, tc := range tests {
		result := d.Detect([]byte(tc.peek))
		if result != tc.expected {
			t.Errorf("Detect(%q) = %v, expected %v", tc.peek, result, tc.expected)
		}
	}

	if d.BytesNeeded() != 4 {
		t.Errorf("BytesNeeded() = %d, expected 4", d.BytesNeeded())
	}
}

func TestStompDestToTopic(t *testing.T) {
	tests := []struct {
		dest     string
		expected string
	}{
		{"/topic/my-topic", "my-topic"},
		{"/queue/my-queue", "my-queue"},
		{"/topic/orders", "orders"},
		{"/queue/work-items", "work-items"},
		{"/exchange/events/routing", "events"},
		{"plain-topic", "plain-topic"},
	}

	for _, tc := range tests {
		result := StompDestToTopic(tc.dest)
		if result != tc.expected {
			t.Errorf("StompDestToTopic(%q) = %q, expected %q", tc.dest, result, tc.expected)
		}
	}
}

func TestTopicToStompDest(t *testing.T) {
	if result := TopicToStompDest("my-topic", false); result != "/topic/my-topic" {
		t.Errorf("TopicToStompDest(my-topic, false) = %q, expected /topic/my-topic", result)
	}

	if result := TopicToStompDest("my-queue", true); result != "/queue/my-queue" {
		t.Errorf("TopicToStompDest(my-queue, true) = %q, expected /queue/my-queue", result)
	}
}
