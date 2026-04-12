package nats

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command represents a NATS protocol command.
type Command string

const (
	// Client commands
	CmdConnect Command = "CONNECT"
	CmdPub     Command = "PUB"
	CmdSub     Command = "SUB"
	CmdUnsub   Command = "UNSUB"
	CmdPing    Command = "PING"
	CmdPong    Command = "PONG"
	CmdSubOpts Command = "SUBOPTS"

	// Server commands
	CmdInfo    Command = "INFO"
	CmdMsg     Command = "MSG"
	CmdOk      Command = "+OK"
	CmdErr     Command = "-ERR"
	CmdPingSrv Command = "PING"
	CmdPongSrv Command = "PONG"
)

// Message represents a NATS protocol message.
type Message struct {
	Command Command
	Args    []string
	Headers map[string]string
	Payload []byte
}

// NewMessage creates a new NATS message.
func NewMessage(cmd Command) *Message {
	return &Message{
		Command: cmd,
		Args:    make([]string, 0),
		Headers: make(map[string]string),
	}
}

// Encode encodes the message to wire format.
func (m *Message) Encode() []byte {
	var buf bytes.Buffer

	// Command
	buf.WriteString(string(m.Command))

	// Arguments
	for _, arg := range m.Args {
		buf.WriteByte(' ')
		buf.WriteString(arg)
	}

	// Headers (if present, in HPUB/HMSG format)
	if len(m.Headers) > 0 {
		buf.WriteString("\r\n")
		for k, v := range m.Headers {
			buf.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
		}
		buf.WriteString("\r\n")
	}

	// Payload
	if len(m.Payload) > 0 {
		buf.WriteString(fmt.Sprintf(" %d\r\n", len(m.Payload)))
		buf.Write(m.Payload)
		buf.WriteString("\r\n")
	} else {
		buf.WriteString("\r\n")
	}

	return buf.Bytes()
}

// String returns a string representation (for logging).
func (m *Message) String() string {
	return fmt.Sprintf("%s %v", m.Command, m.Args)
}

// ReadMessage reads a NATS message from the reader.
func ReadMessage(r *bufio.Reader) (*Message, error) {
	// Read command line
	line, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("read line: %w", err)
	}

	// Trim CRLF
	line = strings.TrimSpace(line)
	if line == "" {
		return ReadMessage(r)
	}

	// Parse command and arguments
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	msg := &Message{
		Command: Command(parts[0]),
		Args:    parts[1:],
		Headers: make(map[string]string),
	}

	// Handle payload for PUB command
	if msg.Command == CmdPub && len(msg.Args) >= 2 {
		// PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
		payloadLen, err := strconv.Atoi(msg.Args[len(msg.Args)-1])
		if err == nil && payloadLen > 0 {
			msg.Payload = make([]byte, payloadLen)
			if _, err := io.ReadFull(r, msg.Payload); err != nil {
				return nil, fmt.Errorf("read payload: %w", err)
			}
			// Read trailing CRLF
			if _, err := r.ReadString('\n'); err != nil {
				return nil, fmt.Errorf("read trailer: %w", err)
			}
		}
	}

	return msg, nil
}

// IsClientCommand returns true if the command is a client command.
func IsClientCommand(cmd Command) bool {
	switch cmd {
	case CmdConnect, CmdPub, CmdSub, CmdUnsub, CmdPing, CmdPong, CmdSubOpts:
		return true
	}
	return false
}

// IsServerCommand returns true if the command is a server command.
func IsServerCommand(cmd Command) bool {
	switch cmd {
	case CmdInfo, CmdMsg, CmdOk, CmdErr, CmdPingSrv, CmdPongSrv:
		return true
	}
	return false
}
