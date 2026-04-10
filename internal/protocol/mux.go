package protocol

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/chimeramq/chimera/internal/broker"
)

// ProtocolDetector determines the protocol from the first bytes of a connection.
type ProtocolDetector interface {
	// Detect returns true if the peeked bytes match this protocol.
	Detect(peek []byte) bool
	// BytesNeeded returns how many bytes are needed for detection.
	BytesNeeded() int
}

// ProtocolHandler handles connections for a specific protocol.
type ProtocolHandler interface {
	// HandleConnection processes a single connection.
	// peeked contains the bytes already read for protocol detection.
	HandleConnection(conn net.Conn, peeked []byte) error
	// Stop gracefully shuts down the handler.
	Stop()
}

// detectorEntry pairs a detector with its handler.
type detectorEntry struct {
	detector ProtocolDetector
	handler  ProtocolHandler
}

// ProtocolMux multiplexes multiple protocols on a single TCP listener.
type ProtocolMux struct {
	broker    *broker.Broker
	listener  net.Listener
	detectors []detectorEntry
	tlsConfig *tls.Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	connections atomic.Int64
}

// NewProtocolMux creates a new protocol multiplexer.
func NewProtocolMux(b *broker.Broker) *ProtocolMux {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProtocolMux{
		broker: b,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Register adds a protocol detector+handler pair.
// Detection order follows registration order.
func (m *ProtocolMux) Register(detector ProtocolDetector, handler ProtocolHandler) {
	m.detectors = append(m.detectors, detectorEntry{
		detector: detector,
		handler:  handler,
	})
}

// Serve starts accepting connections on the configured listener.
func (m *ProtocolMux) Serve() error {
	cfg := m.broker.Config()
	addr := fmt.Sprintf("%s:%d", cfg.Listener.Bind, cfg.Listener.Port)

	var err error
	if cfg.TLS.Enabled {
		cert, cerr := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if cerr != nil {
			return fmt.Errorf("load TLS cert: %w", cerr)
		}
		m.tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		if cfg.TLS.Mutual {
			caPEM, rerr := os.ReadFile(cfg.TLS.ClientCA)
			if rerr != nil {
				return fmt.Errorf("read client CA: %w", rerr)
			}
			clientCAs := x509.NewCertPool()
			if !clientCAs.AppendCertsFromPEM(caPEM) {
				return fmt.Errorf("failed to parse client CA certificates")
			}
			m.tlsConfig.ClientCAs = clientCAs
			m.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		m.listener, err = tls.Listen("tcp", addr, m.tlsConfig)
		if err != nil {
			return fmt.Errorf("listen TLS: %w", err)
		}
	} else {
		m.listener, err = net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}

	m.broker.Logger().Info("protocol multiplexer listening",
		"addr", addr,
		"protocols", len(m.detectors),
		"tls", cfg.TLS.Enabled,
	)

	sem := make(chan struct{}, cfg.Limits.MaxConnections)

	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.ctx.Done():
				return nil
			default:
				slog.Error("accept error", "err", err)
				continue
			}
		}

		select {
		case sem <- struct{}{}:
		default:
			conn.Close()
			continue
		}

		m.wg.Add(1)
		m.connections.Add(1)
		go func() {
			defer m.wg.Done()
			defer m.connections.Add(-1)
			defer func() { <-sem }()
			m.routeConnection(conn)
		}()
	}
}

// routeConnection peeks at the first bytes and routes to the matching handler.
func (m *ProtocolMux) routeConnection(conn net.Conn) {
	defer conn.Close()

	// Determine max bytes needed across all detectors
	maxNeeded := 8 // minimum for protocol detection
	for _, entry := range m.detectors {
		if entry.detector.BytesNeeded() > maxNeeded {
			maxNeeded = entry.detector.BytesNeeded()
		}
	}

	// Use a buffered reader so peeked bytes are not lost
	br := bufio.NewReaderSize(conn, maxNeeded+256)

	peeked, err := br.Peek(maxNeeded)
	if err != nil {
		// Connection closed before sending data
		return
	}

	// Run detectors in order
	for _, entry := range m.detectors {
		n := entry.detector.BytesNeeded()
		if len(peeked) >= n && entry.detector.Detect(peeked[:n]) {
			// Replace conn with the buffered reader wrapper so peeked bytes are available
			bufConn := &bufferedConn{Conn: conn, reader: br}
			entry.handler.HandleConnection(bufConn, peeked[:n])
			return
		}
	}

	// No protocol matched — close with a hint
	conn.Write([]byte("ERR no matching protocol\n"))
}

// Stop gracefully shuts down the multiplexer and all handlers.
func (m *ProtocolMux) Stop() {
	m.cancel()
	if m.listener != nil {
		m.listener.Close()
	}
	for _, entry := range m.detectors {
		entry.handler.Stop()
	}
	m.wg.Wait()
}

// Connections returns the current number of active connections.
func (m *ProtocolMux) Connections() int64 {
	return m.connections.Load()
}

// bufferedConn wraps a net.Conn with a buffered reader.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

// Read reads from the buffered reader.
func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}
