package nats

import (
	"bufio"
	"bytes"
	"testing"
)

func TestMessageEncode(t *testing.T) {
	msg := NewMessage(CmdInfo)
	msg.Args = []string{"{\"version\":\"1.0.0\"}"}

	data := msg.Encode()
	if len(data) == 0 {
		t.Fatal("encoded message is empty")
	}

	// Should contain command
	if !bytes.Contains(data, []byte("INFO")) {
		t.Error("message should contain INFO command")
	}
}

func TestReadMessage(t *testing.T) {
	// Create a simple NATS message
	input := "CONNECT {\"name\":\"test\",\"lang\":\"go\"}\r\n"
	reader := bufio.NewReader(bytes.NewReader([]byte(input)))

	msg, err := ReadMessage(reader)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if msg.Command != CmdConnect {
		t.Errorf("expected command CONNECT, got %s", msg.Command)
	}

	if len(msg.Args) < 1 {
		t.Error("expected at least one argument")
	}
}

func TestReadPubMessage(t *testing.T) {
	// Create a NATS PUB message with payload
	input := "PUB test.subject 11\r\nHello World\r\n"
	reader := bufio.NewReader(bytes.NewReader([]byte(input)))

	msg, err := ReadMessage(reader)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if msg.Command != CmdPub {
		t.Errorf("expected command PUB, got %s", msg.Command)
	}

	if string(msg.Payload) != "Hello World" {
		t.Errorf("expected payload 'Hello World', got '%s'", string(msg.Payload))
	}
}

func TestIsClientCommand(t *testing.T) {
	clientCmds := []Command{CmdConnect, CmdPub, CmdSub, CmdUnsub, CmdPing, CmdPong}

	for _, cmd := range clientCmds {
		if !IsClientCommand(cmd) {
			t.Errorf("%s should be a client command", cmd)
		}
	}

	// Server commands should not be client commands
	serverCmds := []Command{CmdInfo, CmdMsg, CmdOk, CmdErr}
	for _, cmd := range serverCmds {
		if IsClientCommand(cmd) {
			t.Errorf("%s should not be a client command", cmd)
		}
	}
}

func TestIsServerCommand(t *testing.T) {
	serverCmds := []Command{CmdInfo, CmdMsg, CmdOk, CmdErr, CmdPingSrv, CmdPongSrv}

	for _, cmd := range serverCmds {
		if !IsServerCommand(cmd) {
			t.Errorf("%s should be a server command", cmd)
		}
	}
}
