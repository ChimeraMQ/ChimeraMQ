package amqp

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

type nopConn struct{}

func (n *nopConn) Read(b []byte) (int, error)         { return 0, nil }
func (n *nopConn) Write(b []byte) (int, error)        { return len(b), nil }
func (n *nopConn) Close() error                       { return nil }
func (n *nopConn) LocalAddr() net.Addr                { return nil }
func (n *nopConn) RemoteAddr() net.Addr               { return nil }
func (n *nopConn) SetDeadline(t time.Time) error      { return nil }
func (n *nopConn) SetReadDeadline(t time.Time) error  { return nil }
func (n *nopConn) SetWriteDeadline(t time.Time) error { return nil }

func newAMQPTestBroker(t *testing.T) (*broker.Broker, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "amqp-test-*")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
	}
	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return bkr, func() {
		bkr.Stop()
		os.RemoveAll(dir)
	}
}

func newTestConn(bkr *broker.Broker) (*amqpConn, *bytes.Buffer) {
	var buf bytes.Buffer
	ac := &amqpConn{
		server:   NewServer(bkr),
		conn:     &nopConn{},
		reader:   bufio.NewReader(bytes.NewReader(nil)),
		writer:   bufio.NewWriter(&buf),
		maxSize:  defaultMaxFrameSize,
		channels: make(map[uint16]*amqpChannel),
	}
	return ac, &buf
}

// buildFlowBody creates a minimal FLOW performative body.
func buildFlowBody(handle uint32, credit uint32) []byte {
	return buildDescribedList(descFlow, []interface{}{
		handle,    // handle
		uint32(0), // delivery-count
		credit,    // link-credit
		nil,       // available
	})
}

// buildTransferBody creates a minimal TRANSFER performative body.
func buildTransferBody(handle uint32) []byte {
	return buildDescribedList(descTransfer, []interface{}{
		handle,    // handle
		uint32(1), // delivery-id
		nil,       // delivery-tag
	})
}

// parseValue extracts the value portion from a described type body.
func parseValue(body []byte) []byte {
	_, value, err := ParseDescribedType(body)
	if err != nil {
		return body
	}
	return value
}
func TestHandleOpen(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, buf := newTestConn(bkr)
	openBody := BuildOpen("client-1", "test-host")
	err := ac.handleOpen(openBody, 0)
	if err != nil {
		t.Fatalf("handleOpen: %v", err)
	}
	ac.writer.Flush()

	if buf.Len() == 0 {
		t.Error("expected response frame written")
	}
	if ac.containerID == "" {
		t.Error("expected containerID to be set")
	}
}

func TestHandleBegin(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, buf := newTestConn(bkr)
	beginBody := BuildBegin(0, 0, 65535, 65535, 4294967295)
	err := ac.handleBegin(beginBody, 1)
	if err != nil {
		t.Fatalf("handleBegin: %v", err)
	}
	ac.writer.Flush()

	if buf.Len() == 0 {
		t.Error("expected BEGIN response")
	}
	if _, ok := ac.channels[1]; !ok {
		t.Error("channel 1 should be created")
	}
}

func TestHandleAttach(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, buf := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{remoteChannel: 0, links: make(map[uint32]*amqpLink)}

	attachBody := BuildAttach("sender-link", 0, 0, "test-topic")
	err := ac.handleAttach(parseValue(attachBody), 0)
	if err != nil {
		t.Fatalf("handleAttach: %v", err)
	}
	ac.writer.Flush()

	if buf.Len() == 0 {
		t.Error("expected ATTACH response")
	}
	ch := ac.channels[0]
	if _, ok := ch.links[0]; !ok {
		t.Error("link 0 should be created")
	}
	if ch.links[0].name != "sender-link" {
		t.Errorf("link name = %q", ch.links[0].name)
	}
}

func TestHandleAttachNoChannel(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	attachBody := BuildAttach("link", 0, 0, "topic")
	err := ac.handleAttach(attachBody, 5)
	if err == nil {
		t.Error("expected error for missing channel")
	}
}

func TestHandleFlow(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "link", handle: 0, role: 1},
		},
	}

	flowBody := buildFlowBody(0, 100)
	err := ac.handleFlow(parseValue(flowBody), 0)
	if err != nil {
		t.Fatalf("handleFlow: %v", err)
	}
	if ac.channels[0].links[0].credit != 100 {
		t.Errorf("credit = %d, want 100", ac.channels[0].links[0].credit)
	}
}

func TestHandleDetach(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "link", handle: 0},
		},
	}

	detachBody := BuildDetach(0, true)
	err := ac.handleDetach(parseValue(detachBody), 0)
	if err != nil {
		t.Fatalf("handleDetach: %v", err)
	}
	ac.writer.Flush()

	if _, ok := ac.channels[0].links[0]; ok {
		t.Error("link 0 should be removed after detach")
	}
}

func TestHandleEnd(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[1] = &amqpChannel{remoteChannel: 1, links: make(map[uint32]*amqpLink)}

	err := ac.handleEnd(1)
	if err != nil {
		t.Fatalf("handleEnd: %v", err)
	}
	ac.writer.Flush()

	if _, ok := ac.channels[1]; ok {
		t.Error("channel 1 should be removed after end")
	}
}

func TestHandleClose(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, buf := newTestConn(bkr)
	err := ac.handleClose()
	if err == nil {
		t.Error("handleClose should return error")
	}
	ac.writer.Flush()
	if buf.Len() == 0 {
		t.Error("expected CLOSE frame written")
	}
}

func TestHandleTransferPublish(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name: "amqp-topic", Mode: broker.ModeUnified, Partitions: 1,
	})

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "sender", handle: 0, role: 0, addr: "amqp-topic"},
		},
	}

	transferBody := buildTransferBody(0)
	err := ac.handleTransfer(parseValue(transferBody), 0)
	if err != nil {
		t.Fatalf("handleTransfer: %v", err)
	}
}

func TestHandleTransferNoChannel(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	err := ac.handleTransfer(parseValue(buildTransferBody(0)), 99)
	if err != nil {
		t.Fatalf("expected nil for missing channel, got: %v", err)
	}
}

func TestHandleTransferNoSenderLink(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "receiver", handle: 0, role: 1, addr: "topic"},
		},
	}

	err := ac.handleTransfer(parseValue(buildTransferBody(0)), 0)
	if err != nil {
		t.Fatalf("expected nil for no sender link, got: %v", err)
	}
}

func TestHandleDisposition(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "sender", handle: 0, role: 0, delivered: 5},
		},
	}

	dispositionBody := BuildDisposition(1, 0, 10, true, "accepted")
	err := ac.handleDisposition(parseValue(dispositionBody), 0)
	if err != nil {
		t.Fatalf("handleDisposition: %v", err)
	}
	if ac.channels[0].links[0].delivered != 10 {
		t.Errorf("delivered = %d, want 10", ac.channels[0].links[0].delivered)
	}
}

func TestSplitNull(t *testing.T) {
	parts := splitNull("\x00user\x00pass")
	if len(parts) != 3 {
		t.Fatalf("parts = %d, want 3", len(parts))
	}
	if parts[1] != "user" || parts[2] != "pass" {
		t.Errorf("parts = %v", parts)
	}
}

func TestExtractPayload(t *testing.T) {
	payload := extractPayload([]byte{typeNull})
	if len(payload) == 0 {
		t.Error("expected non-empty payload")
	}
}

func TestHandleFrameUnknownDescriptor(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	body := []byte{0x00, typeUlong, 0, 0, 0, 0, 0xFF, 0xFF, 0x45, 0x00, 0x00, 0x00, 0x00, typeNull}
	err := ac.handleFrame(&Frame{Type: frameTypeAMQP, Channel: 0, Body: body})
	if err == nil {
		t.Error("expected error for unknown descriptor")
	}
}

func TestFullSessionLifecycle(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, buf := newTestConn(bkr)

	// OPEN
	ac.handleOpen(BuildOpen("client", "host"), 0)

	// BEGIN
	ac.handleBegin(BuildBegin(0, 0, 65535, 65535, 4294967295), 0)

	// ATTACH receiver
	ac.handleAttach(parseValue(BuildAttach("my-link", 0, 1, "test-topic")), 0)

	// FLOW credit=50
	ac.handleFlow(parseValue(buildFlowBody(0, 50)), 0)

	// DISPOSITION
	ac.handleDisposition(parseValue(BuildDisposition(1, 0, 10, true, "accepted")), 0)

	// DETACH
	ac.handleDetach(parseValue(BuildDetach(0, true)), 0)

	// END
	ac.handleEnd(0)

	// CLOSE
	ac.handleClose()

	ac.writer.Flush()
	if buf.Len() == 0 {
		t.Error("expected data from full lifecycle")
	}
}
