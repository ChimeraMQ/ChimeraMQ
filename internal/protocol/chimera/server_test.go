package chimera

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

// testHarness sets up a broker + chimera server + client pipe for testing.
type testHarness struct {
	broker  *broker.Broker
	server  *Server
	client  net.Conn
	clientR *bufio.Reader
	clientW *bufio.Writer
	cleanup func()
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	dir := t.TempDir()
	cfg, err := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0 // let OS pick
	cfg.Listener.AdminPort = 0

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	srv, err := NewServer(b)
	if err != nil {
		b.Stop()
		t.Fatalf("NewServer: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		srv.Serve()
	}()
	// Give listener a moment
	time.Sleep(10 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", srv.listener.Addr().String(), 2*time.Second)
	if err != nil {
		srv.StopAccepting()
		b.Stop()
		t.Fatalf("dial: %v", err)
	}

	h := &testHarness{
		broker:  b,
		server:  srv,
		client:  conn,
		clientR: bufio.NewReaderSize(conn, 64*1024),
		clientW: bufio.NewWriterSize(conn, 64*1024),
	}
	h.cleanup = func() {
		conn.Close()
		srv.StopAccepting()
		wg.Wait()
		b.Stop()
	}

	return h
}

func sendConnect(h *testHarness, clientID string) error {
	payload := encodeConnectPayload(clientID, "user", "pass", 60)
	frame := &Frame{Version: FrameVersion, OpCode: OpConnect, Payload: payload}
	data, err := EncodeFrame(frame)
	if err != nil {
		return err
	}
	h.clientW.Write(data)
	return h.clientW.Flush()
}

func encodeConnectPayload(clientID, username, password string, keepalive uint16) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(clientID)))
	buf = append(buf, clientID...)
	buf = appendUint16(buf, uint16(len(username)))
	buf = append(buf, username...)
	buf = appendUint16(buf, uint16(len(password)))
	buf = append(buf, password...)
	buf = appendUint16(buf, keepalive)
	return buf
}

func readFrame(r *bufio.Reader) (*Frame, error) {
	return DecodeFrame(r)
}

func TestServerConnectAndConnAck(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	if err := sendConnect(h, "test-client"); err != nil {
		t.Fatalf("send connect: %v", err)
	}

	frame, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if frame.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", frame.OpCode)
	}

	r := newReader(frame.Payload)
	id, _ := r.readString()
	if id == "" {
		t.Error("expected non-empty client ID in connack")
	}
}

func TestServerConnectWithEmptyClientID(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	if err := sendConnect(h, ""); err != nil {
		t.Fatalf("send connect: %v", err)
	}

	frame, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if frame.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", frame.OpCode)
	}

	// Server should auto-assign a client ID
	r := newReader(frame.Payload)
	id, _ := r.readString()
	if id == "" {
		t.Error("server should assign an auto-generated client ID")
	}
}

func TestServerPingPong(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "ping-test")
	readFrame(h.clientR) // consume connack

	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

func TestServerCreateTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "create-test")
	readFrame(h.clientR) // connack

	var payload []byte
	payload = appendUint16(payload, uint16(len("tcp-topic")))
	payload = append(payload, "tcp-topic"...)
	payload = appendUint16(payload, uint16(len("stream")))
	payload = append(payload, "stream"...)
	payload = appendUint32(payload, 4)

	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck (create ack)", resp.OpCode)
	}

	// Verify topic exists
	topic, ok := h.broker.Topics().GetTopic("tcp-topic")
	if !ok {
		t.Fatal("topic not found")
	}
	if topic.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", topic.Partitions)
	}
}

func TestServerPublish(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "pub-test")
	readFrame(h.clientR) // connack

	// Create topic first
	createTopicPayload := encodeCreateTopicPayload("pub-topic", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: createTopicPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // consume ack

	// Publish message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("pub-topic")))
	pubPayload = append(pubPayload, "pub-topic"...)
	pubPayload = appendUint16(pubPayload, 0) // empty routing key
	pubPayload = append(pubPayload, 0)       // priority
	pubPayload = appendUint64(pubPayload, 0) // TTL
	pubPayload = appendUint64(pubPayload, 0) // DeliverAt
	pubPayload = append(pubPayload, []byte("hello tcp")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	if resp.OpCode != OpPubAck {
		t.Errorf("opcode = %v, want OpPubAck", resp.OpCode)
	}

	r := newReader(resp.Payload)
	topic, _ := r.readString()
	if topic != "pub-topic" {
		t.Errorf("topic = %q, want pub-topic", topic)
	}
	partID := binary.BigEndian.Uint32(r.read(4))
	if partID > 1 {
		t.Errorf("partition = %d, expected 0 or 1", partID)
	}
	offset := binary.BigEndian.Uint64(r.read(8))
	// First message in partition gets offset 0 — just verify we read it
	_ = offset
}

func TestServerSubscribe(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "sub-test")
	readFrame(h.clientR) // connack

	var payload []byte
	payload = appendUint16(payload, uint16(len("sub-topic")))
	payload = append(payload, "sub-topic"...)
	payload = append(payload, 1) // mode
	payload = appendUint16(payload, uint16(len("my-group")))
	payload = append(payload, "my-group"...)
	payload = appendUint32(payload, 10)
	payload = appendUint64(payload, 0)

	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubscribe, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck", resp.OpCode)
	}

	r := newReader(resp.Payload)
	topic, _ := r.readString()
	if topic != "sub-topic" {
		t.Errorf("topic = %q, want sub-topic", topic)
	}
	success := r.read(1)[0]
	if success != 1 {
		t.Error("expected success=1")
	}
}

func TestServerDeleteTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "del-test")
	readFrame(h.clientR) // connack

	// Create topic first
	ctPayload := encodeCreateTopicPayload("del-topic", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Delete it
	var delPayload []byte
	delPayload = appendUint16(delPayload, uint16(len("del-topic")))
	delPayload = append(delPayload, "del-topic"...)

	delFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDeleteTopic, Payload: delPayload})
	h.clientW.Write(delFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck", resp.OpCode)
	}

	// Verify deleted
	_, ok := h.broker.Topics().GetTopic("del-topic")
	if ok {
		t.Error("topic should be deleted")
	}
}

func TestServerDisconnect(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "disc-test")
	readFrame(h.clientR) // connack

	// Send DISCONNECT — server should close the connection
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDisconnect})
	h.clientW.Write(frame)
	h.clientW.Flush()

	// Next read should return EOF (connection closed by server)
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err != io.EOF {
		// On some systems it may be a different error, but should be an error
		if err == nil {
			t.Error("expected EOF or error after disconnect")
		}
	}
}

func TestServerNonConnectFirstFrame(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Send PUBLISH without CONNECT first
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: []byte("data")})
	h.clientW.Write(frame)
	h.clientW.Flush()

	// Server should close connection
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err == nil {
		t.Error("expected error when non-CONNECT frame sent first")
	}
}

func TestServerFetchAfterPublish(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-test")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("fetch-t", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Publish a message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("fetch-t")))
	pubPayload = append(pubPayload, "fetch-t"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("fetched-data")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// Fetch
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("fetch-t")))
	fetchPayload = append(fetchPayload, "fetch-t"...)
	fetchPayload = appendUint32(fetchPayload, 0)  // partition
	fetchPayload = appendUint64(fetchPayload, 0)  // offset
	fetchPayload = appendUint32(fetchPayload, 10) // max messages

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read fetch resp: %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("opcode = %v, want OpFetchResp", resp.OpCode)
	}

	r := newReader(resp.Payload)
	count := binary.BigEndian.Uint32(r.read(4))
	if count != 1 {
		t.Fatalf("message count = %d, want 1", count)
	}
	msgLen := binary.BigEndian.Uint32(r.read(4))
	msgData := r.read(int(msgLen))
	if len(msgData) == 0 {
		t.Error("expected non-empty message data")
	}
}

func TestServerCommitOffset(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "commit-test")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("commit-t", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Commit offset
	var commitPayload []byte
	commitPayload = appendUint16(commitPayload, uint16(len("test-group")))
	commitPayload = append(commitPayload, "test-group"...)
	commitPayload = appendUint16(commitPayload, uint16(len("commit-t")))
	commitPayload = append(commitPayload, "commit-t"...)
	commitPayload = appendUint32(commitPayload, 0) // partition
	commitPayload = appendUint64(commitPayload, 5) // offset

	commitFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitOffset, Payload: commitPayload})
	h.clientW.Write(commitFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read commit ack: %v", err)
	}
	if resp.OpCode != OpCommitAck {
		t.Errorf("opcode = %v, want OpCommitAck", resp.OpCode)
	}
}

func TestServerMultipleClients(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Client 1 already connected
	sendConnect(h, "client1")
	readFrame(h.clientR) // connack

	// Connect client 2
	conn2, err := net.DialTimeout("tcp", h.server.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial client2: %v", err)
	}
	defer conn2.Close()

	r2 := bufio.NewReaderSize(conn2, 64*1024)
	w2 := bufio.NewWriterSize(conn2, 64*1024)

	connectPayload := encodeConnectPayload("client2", "", "", 0)
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: connectPayload})
	w2.Write(frame)
	w2.Flush()

	resp, err := readFrame(r2)
	if err != nil {
		t.Fatalf("read connack2: %v", err)
	}
	if resp.OpCode != OpConnAck {
		t.Errorf("client2 connack opcode = %v, want OpConnAck", resp.OpCode)
	}
}

func encodeCreateTopicPayload(name, mode string, partitions uint32) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(name)))
	buf = append(buf, name...)
	buf = appendUint16(buf, uint16(len(mode)))
	buf = append(buf, mode...)
	buf = appendUint32(buf, partitions)
	return buf
}

func TestNewServerInvalidBindAddress(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "256.256.256.256" // invalid IP
	cfg.Listener.Port = 9999

	b, _ := broker.NewBroker(cfg)
	b.Start()
	defer b.Stop()

	_, err := NewServer(b)
	if err == nil {
		t.Error("expected error for invalid bind address")
	}
}

func TestServeAfterListenerClosed(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0

	b, _ := broker.NewBroker(cfg)
	b.Start()
	defer b.Stop()

	srv, err := NewServer(b)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Close listener first, then Serve should return after ctx cancelled
	srv.StopAccepting()

	// Serve should return (either nil from ctx.Done or continue then return)
	done := make(chan error, 1)
	go func() {
		done <- srv.Serve()
	}()

	select {
	case err := <-done:
		// Should return nil (ctx cancelled)
		if err != nil {
			t.Logf("Serve returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("Serve should return after StopAccepting")
	}
}

func TestServerFetchWithShortPayload(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-short")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("fs-topic", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Fetch with only topic name (no partition/offset/limit fields)
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("fs-topic")))
	fetchPayload = append(fetchPayload, "fs-topic"...)
	// No additional fields — defaults should be used

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("fetch resp: %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("opcode = %v, want OpFetchResp", resp.OpCode)
	}
}

func TestServerAckAndNack(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "ack-test")
	readFrame(h.clientR) // connack

	// Create queue topic with DLQ
	var ctPayload []byte
	ctPayload = appendUint16(ctPayload, uint16(len("ack-topic")))
	ctPayload = append(ctPayload, "ack-topic"...)
	ctPayload = appendUint16(ctPayload, uint16(len("queue")))
	ctPayload = append(ctPayload, "queue"...)
	ctPayload = appendUint32(ctPayload, 1)

	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Subscribe to the topic to register consumer
	var subPayload []byte
	subPayload = appendUint16(subPayload, uint16(len("ack-topic")))
	subPayload = append(subPayload, "ack-topic"...)
	subPayload = append(subPayload, 1) // mode
	subPayload = appendUint16(subPayload, uint16(len("ack-group")))
	subPayload = append(subPayload, "ack-group"...)
	subPayload = appendUint32(subPayload, 10)
	subPayload = appendUint64(subPayload, 0)

	subFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubscribe, Payload: subPayload})
	h.clientW.Write(subFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // suback

	// Publish a message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("ack-topic")))
	pubPayload = append(pubPayload, "ack-topic"...)
	pubPayload = appendUint16(pubPayload, 0) // routing key
	pubPayload = append(pubPayload, 0)       // priority
	pubPayload = appendUint64(pubPayload, 0) // TTL
	pubPayload = appendUint64(pubPayload, 0) // DeliverAt
	pubPayload = append(pubPayload, []byte("test-msg")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// Send ACK for offset 0
	var ackPayload []byte
	ackPayload = appendUint16(ackPayload, uint16(len("ack-topic")))
	ackPayload = append(ackPayload, "ack-topic"...)
	ackPayload = appendUint32(ackPayload, 0) // partition
	ackPayload = appendUint64(ackPayload, 0) // offset

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpAck, Payload: ackPayload})
	h.clientW.Write(ackFrame)
	h.clientW.Flush()
	// ACK doesn't send a response — just verify no hang
}

func TestServerNackNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "nack-test")
	readFrame(h.clientR) // connack

	// Send NACK for a topic that doesn't exist
	var nackPayload []byte
	nackPayload = appendUint16(nackPayload, uint16(len("no-topic")))
	nackPayload = append(nackPayload, "no-topic"...)
	nackPayload = appendUint32(nackPayload, 0)
	nackPayload = appendUint64(nackPayload, 0)

	nackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpNack, Payload: nackPayload})
	h.clientW.Write(nackFrame)
	h.clientW.Flush()
	// NACK doesn't send response — just verify no crash
}

func TestServerPublishError(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "pub-err-test")
	readFrame(h.clientR) // connack

	// Publish to nonexistent topic
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("noexist")))
	pubPayload = append(pubPayload, "noexist"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("data")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError", resp.OpCode)
	}
}

func TestServerSubscribeError(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "sub-err-test")
	readFrame(h.clientR) // connack

	// Subscribe with very short payload (truncated)
	var subPayload []byte
	subPayload = appendUint16(subPayload, uint16(len("x")))
	subPayload = append(subPayload, "x"...)
	// Missing mode, group, batch, offset — should still work with defaults

	subFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubscribe, Payload: subPayload})
	h.clientW.Write(subFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// Should get SubAck (even with defaults)
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck", resp.OpCode)
	}
}

func TestServerDisconnectAll(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "disc-all-1")
	readFrame(h.clientR) // connack

	// Connect second client
	conn2, _ := net.DialTimeout("tcp", h.server.listener.Addr().String(), 2*time.Second)
	defer conn2.Close()
	r2 := bufio.NewReaderSize(conn2, 64*1024)
	w2 := bufio.NewWriterSize(conn2, 64*1024)

	connectPayload := encodeConnectPayload("disc-all-2", "", "", 0)
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: connectPayload})
	w2.Write(frame)
	w2.Flush()
	readFrame(r2) // connack

	// Disconnect all clients
	h.server.DisconnectAll()

	// Both connections should be closed
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err == nil {
		t.Error("expected error after DisconnectAll")
	}
}

func TestServerServeAcceptTransientError(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0

	b, _ := broker.NewBroker(cfg)
	b.Start()
	defer b.Stop()

	srv, _ := NewServer(b)

	// Close listener to cause transient Accept error, then cancel ctx
	srv.listener.Close()

	done := make(chan error, 1)
	go func() {
		done <- srv.Serve()
	}()

	// Give it a moment to hit Accept error → default: continue → loop again
	time.Sleep(50 * time.Millisecond)

	// Now cancel context to make it stop
	srv.StopAccepting()

	select {
	case <-done:
		// Serve returned
	case <-time.After(3 * time.Second):
		t.Error("Serve should return after StopAccepting")
	}
}

func TestServerHandleCommitOffsetDetailed(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "commit-det")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("commit-det-t", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Commit with a short payload (only group + topic, no partition/offset)
	var commitPayload []byte
	commitPayload = appendUint16(commitPayload, uint16(len("mygroup")))
	commitPayload = append(commitPayload, "mygroup"...)
	commitPayload = appendUint16(commitPayload, uint16(len("commit-det-t")))
	commitPayload = append(commitPayload, "commit-det-t"...)
	// Missing partition and offset — should use defaults

	commitFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitOffset, Payload: commitPayload})
	h.clientW.Write(commitFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read commit ack: %v", err)
	}
}

func TestServerFetchWithValidTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-valid")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("fetch-valid-t", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Publish two messages
	for i := 0; i < 2; i++ {
		var pubPayload []byte
		pubPayload = appendUint16(pubPayload, uint16(len("fetch-valid-t")))
		pubPayload = append(pubPayload, "fetch-valid-t"...)
		pubPayload = appendUint16(pubPayload, 0)
		pubPayload = append(pubPayload, 0)
		pubPayload = appendUint64(pubPayload, 0)
		pubPayload = appendUint64(pubPayload, 0)
		pubPayload = append(pubPayload, "msg-data"...)
		pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
		h.clientW.Write(pubFrame)
		h.clientW.Flush()
		readFrame(h.clientR) // puback
	}

	// Fetch from partition 0
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("fetch-valid-t")))
	fetchPayload = append(fetchPayload, "fetch-valid-t"...)
	fetchPayload = appendUint32(fetchPayload, 0)
	fetchPayload = appendUint64(fetchPayload, 0)
	fetchPayload = appendUint32(fetchPayload, 10)

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read fetch resp: %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Fatalf("opcode = %v, want OpFetchResp", resp.OpCode)
	}

	r := newReader(resp.Payload)
	count := binary.BigEndian.Uint32(r.read(4))
	if count == 0 {
		t.Error("expected at least 1 message")
	}
}

func TestServerConnectFrameDecodeError(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Write garbage data that will fail frame decoding
	h.clientW.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	h.clientW.Flush()

	// Server should close connection on decode error
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err == nil {
		t.Error("expected error after sending invalid frame data")
	}
}

func TestServerDoubleConnect(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// First connect
	sendConnect(h, "double-connect")
	readFrame(h.clientR) // connack

	// Second connect on same connection
	sendConnect(h, "double-connect-2")
	readFrame(h.clientR) // connack — server accepts re-connect
}

func TestServerCreateTopicDuplicate(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "dup-topic")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("dup-topic", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Create same topic again
	ctFrame2, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame2)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read dup response: %v", err)
	}
	// Should get an error response
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError for duplicate topic", resp.OpCode)
	}
}

func TestServerSubscribeNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "sub-noexist")
	readFrame(h.clientR) // connack

	// Server auto-creates topics on subscribe, so subscribing to a
	// nonexistent topic returns SubAck (auto-created).
	var subPayload []byte
	subPayload = appendUint16(subPayload, uint16(len("auto-topic")))
	subPayload = append(subPayload, "auto-topic"...)
	subPayload = append(subPayload, 1)
	subPayload = appendUint16(subPayload, uint16(len("mygroup")))
	subPayload = append(subPayload, "mygroup"...)
	subPayload = appendUint32(subPayload, 10)
	subPayload = appendUint64(subPayload, 0)

	subFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubscribe, Payload: subPayload})
	h.clientW.Write(subFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck (auto-created)", resp.OpCode)
	}
}

func TestServerFetchNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-noexist")
	readFrame(h.clientR) // connack

	// Fetch from topic that doesn't exist
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("noexist-fetch")))
	fetchPayload = append(fetchPayload, "noexist-fetch"...)
	fetchPayload = appendUint32(fetchPayload, 0)
	fetchPayload = appendUint64(fetchPayload, 0)
	fetchPayload = appendUint32(fetchPayload, 10)

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("opcode = %v, want OpFetchResp (empty result for nonexistent)", resp.OpCode)
	}
}

func TestServerDeleteNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "del-noexist")
	readFrame(h.clientR) // connack

	// Delete topic that doesn't exist
	var delPayload []byte
	delPayload = appendUint16(delPayload, uint16(len("noexist-del")))
	delPayload = append(delPayload, "noexist-del"...)

	delFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDeleteTopic, Payload: delPayload})
	h.clientW.Write(delFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// Should get an error for nonexistent topic
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError", resp.OpCode)
	}
}

func TestServerCommitOffsetNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "commit-noexist")
	readFrame(h.clientR) // connack

	var commitPayload []byte
	commitPayload = appendUint16(commitPayload, uint16(len("mygroup")))
	commitPayload = append(commitPayload, "mygroup"...)
	commitPayload = appendUint16(commitPayload, uint16(len("noexist-commit")))
	commitPayload = append(commitPayload, "noexist-commit"...)
	commitPayload = appendUint32(commitPayload, 0)
	commitPayload = appendUint64(commitPayload, 5)

	commitFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitOffset, Payload: commitPayload})
	h.clientW.Write(commitFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := readFrame(h.clientR)
	// Should get a response (CommitAck or error) without hanging
	if err != nil {
		t.Logf("commit response: %v (may be expected)", err)
	}
}

func TestServerFetchAfterStorageClose(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-closed")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("fc-topic", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Close storage to cause Fetch error
	h.broker.Storage().Close()

	// Fetch should still return a response (error encoded in response)
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("fc-topic")))
	fetchPayload = append(fetchPayload, "fc-topic"...)
	fetchPayload = appendUint32(fetchPayload, 0)
	fetchPayload = appendUint64(fetchPayload, 0)
	fetchPayload = appendUint32(fetchPayload, 10)

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// Should get either FetchResp (empty) or Error
	if resp.OpCode != OpFetchResp && resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpFetchResp or OpError", resp.OpCode)
	}
}
