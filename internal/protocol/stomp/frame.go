package stomp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command represents a STOMP frame command.
type Command string

const (
	// Client commands
	CmdConnect     Command = "CONNECT"
	CmdStomp       Command = "STOMP"
	CmdSend        Command = "SEND"
	CmdSubscribe   Command = "SUBSCRIBE"
	CmdUnsubscribe Command = "UNSUBSCRIBE"
	CmdBegin       Command = "BEGIN"
	CmdCommit      Command = "COMMIT"
	CmdAbort       Command = "ABORT"
	CmdAck         Command = "ACK"
	CmdNack        Command = "NACK"
	CmdDisconnect  Command = "DISCONNECT"

	// Server commands
	CmdConnected Command = "CONNECTED"
	CmdMessage   Command = "MESSAGE"
	CmdReceipt   Command = "RECEIPT"
	CmdError     Command = "ERROR"
	CmdHeartbeat Command = "HEARTBEAT"
)

// Frame represents a STOMP protocol frame.
type Frame struct {
	Command Command
	Headers map[string]string
	Body    []byte
}

// NewFrame creates a new STOMP frame.
func NewFrame(cmd Command) *Frame {
	return &Frame{
		Command: cmd,
		Headers: make(map[string]string),
	}
}

// Get returns a header value with case-insensitive key lookup.
func (f *Frame) Get(key string) string {
	// Try exact match first
	if v, ok := f.Headers[key]; ok {
		return v
	}
	// Try case-insensitive match
	for k, v := range f.Headers {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// Set sets a header value.
func (f *Frame) Set(key, value string) {
	f.Headers[key] = value
}

// Encode encodes the frame to wire format.
func (f *Frame) Encode() []byte {
	var buf bytes.Buffer

	// Command
	buf.WriteString(string(f.Command))
	buf.WriteByte('\n')

	// Headers
	for k, v := range f.Headers {
		buf.WriteString(encodeHeader(k))
		buf.WriteByte(':')
		buf.WriteString(encodeHeader(v))
		buf.WriteByte('\n')
	}

	// Empty line separates headers from body
	buf.WriteByte('\n')

	// Body
	if len(f.Body) > 0 {
		buf.Write(f.Body)
	}

	// NULL terminator
	buf.WriteByte(0)

	return buf.Bytes()
}

// encodeHeader escapes special characters in header values.
func encodeHeader(s string) string {
	// Escape backslash, colon, and newline
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ":", "\\c")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// decodeHeader unescapes special characters in header values.
func decodeHeader(s string) string {
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\c", ":")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}

// ReadFrame reads a STOMP frame from the reader.
func ReadFrame(r *bufio.Reader) (*Frame, error) {
	// Read command line
	cmdLine, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("read command: %w", err)
	}

	cmdLine = strings.TrimSpace(cmdLine)
	if cmdLine == "" {
		// Skip empty lines (heartbeats)
		return ReadFrame(r)
	}

	frame := NewFrame(Command(cmdLine))

	// Read headers
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		line = strings.TrimRight(line, "\n")
		if line == "" {
			// Empty line signals end of headers
			break
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header line: %s", line)
		}

		key := decodeHeader(strings.TrimSpace(parts[0]))
		value := decodeHeader(strings.TrimSpace(parts[1]))
		frame.Headers[key] = value
	}

	// Read body
	contentLengthStr := frame.Get("content-length")
	if contentLengthStr != "" {
		// Fixed-length body
		contentLength, err := strconv.Atoi(contentLengthStr)
		if err != nil {
			return nil, fmt.Errorf("invalid content-length: %w", err)
		}

		frame.Body = make([]byte, contentLength)
		if _, err := io.ReadFull(r, frame.Body); err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}

		// Read NULL terminator
		b, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read terminator: %w", err)
		}
		if b != 0 {
			return nil, errors.New("expected NULL terminator after body")
		}
	} else {
		// Read until NULL terminator
		var body bytes.Buffer
		for {
			b, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("read body byte: %w", err)
			}
			if b == 0 {
				break
			}
			body.WriteByte(b)
		}
		frame.Body = body.Bytes()
	}

	return frame, nil
}

// IsClientCommand returns true if the command is a client command.
func IsClientCommand(cmd Command) bool {
	switch cmd {
	case CmdConnect, CmdStomp, CmdSend, CmdSubscribe, CmdUnsubscribe,
		CmdBegin, CmdCommit, CmdAbort, CmdAck, CmdNack, CmdDisconnect:
		return true
	}
	return false
}

// IsServerCommand returns true if the command is a server command.
func IsServerCommand(cmd Command) bool {
	switch cmd {
	case CmdConnected, CmdMessage, CmdReceipt, CmdError, CmdHeartbeat:
		return true
	}
	return false
}

// StompDestToTopic converts a STOMP destination to a topic name.
// STOMP destinations are like "/topic/my-topic" or "/queue/my-queue".
func StompDestToTopic(dest string) string {
	if strings.HasPrefix(dest, "/topic/") {
		return dest[7:]
	}
	if strings.HasPrefix(dest, "/queue/") {
		return dest[7:]
	}
	if strings.HasPrefix(dest, "/exchange/") {
		parts := strings.SplitN(dest[10:], "/", 2)
		return parts[0]
	}
	// Default: use as-is
	return dest
}

// TopicToStompDest converts a topic name to a STOMP destination.
func TopicToStompDest(topic string, isQueue bool) string {
	if isQueue {
		return "/queue/" + topic
	}
	return "/topic/" + topic
}
