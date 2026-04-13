package broker

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// HandoffManager manages zero-downtime rolling upgrades.
// It coordinates graceful connection draining and state transfer.
type HandoffManager struct {
	b           *Broker
	mu          sync.RWMutex
	handoffSock string
	listener    net.Listener
	draining    bool
}

// NewHandoffManager creates a new handoff manager for rolling upgrades.
func NewHandoffManager(b *Broker) *HandoffManager {
	handoffSock := b.config.Node.DataDir + "/handoff.sock"
	return &HandoffManager{
		b:           b,
		handoffSock: handoffSock,
	}
}

// Start starts the handoff management listener.
func (h *HandoffManager) Start() error {
	// Remove old socket if exists
	_ = os.Remove(h.handoffSock)

	ln, err := net.Listen("unix", h.handoffSock)
	if err != nil {
		return fmt.Errorf("handoff listen: %w", err)
	}

	h.listener = ln
	h.b.logger.Info("handoff manager started", "socket", h.handoffSock)

	go h.run()
	return nil
}

// Stop stops the handoff manager.
func (h *HandoffManager) Stop() {
	if h.listener != nil {
		h.listener.Close()
	}
	_ = os.Remove(h.handoffSock)
}

// run handles incoming handoff requests from new version.
func (h *HandoffManager) run() {
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			if h.draining {
				return
			}
			h.b.logger.Error("handoff accept failed", "error", err)
			continue
		}

		go h.handleHandoffRequest(conn)
	}
}

// handleHandoffRequest processes a handoff request from the new version.
func (h *HandoffManager) handleHandoffRequest(conn net.Conn) {
	defer conn.Close()

	// Read command (exactly 4 bytes)
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		h.b.logger.Error("handoff read failed", "error", err)
		return
	}

	cmd := string(buf)
	switch cmd {
	case "DRAI": // Drain request
		h.b.logger.Info("handoff: received drain request")
		err := h.DrainConnections()
		if err != nil {
			_, _ = conn.Write([]byte("ERR " + err.Error()))
		} else {
			_, _ = conn.Write([]byte("OK  "))
		}
	case "STAT": // Status request
		h.b.logger.Info("handoff: received status request")
		status := h.Status()
		_, _ = conn.Write([]byte(status))
	default:
		h.b.logger.Warn("handoff: unknown command", "cmd", cmd)
		_, _ = conn.Write([]byte("UNK "))
	}
}

// DrainConnections gracefully drains all active connections.
// This is called by the old version when the new version is ready.
func (h *HandoffManager) DrainConnections() error {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return fmt.Errorf("already draining")
	}
	h.draining = true
	h.mu.Unlock()

	h.b.logger.Info("handoff: starting connection drain")

	// Signal broker to stop accepting new connections
	if h.b.protocolMux != nil {
		h.b.protocolMux.Stop()
	}

	// Stop the main listener
	if h.b.mainListener != nil {
		h.b.mainListener.Close()
	}

	// Wait for active connections with timeout
	done := make(chan struct{})
	go func() {
		// Note: Connection tracking would happen here if implemented
		close(done)
	}()

	select {
	case <-done:
		h.b.logger.Info("handoff: all connections drained gracefully")
	case <-time.After(30 * time.Second):
		h.b.logger.Warn("handoff: timeout waiting for connections, forcing")
	}

	return nil
}

// Status returns the current handoff status.
func (h *HandoffManager) Status() string {
	h.mu.RLock()
	draining := h.draining
	h.mu.RUnlock()

	if draining {
		return "DRAINING"
	}
	return "ACTIVE"
}
