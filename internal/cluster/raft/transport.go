package raft

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Transport sends Raft RPCs to peers.
type Transport interface {
	SendAppendEntries(nodeID NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(nodeID NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendInstallSnapshot(nodeID NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// RPCHandler handles incoming Raft RPCs.
type RPCHandler interface {
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse
}

// connEntry tracks a connection and its last use time.
type connEntry struct {
	conn     net.Conn
	lastUsed time.Time
}

// TCPTransport implements Transport over TCP.
type TCPTransport struct {
	mu          sync.RWMutex
	conns       map[NodeID]*connEntry
	addrs       map[NodeID]string
	timeout     time.Duration
	idleTimeout time.Duration
	tlsConfig   *tls.Config
}

// NewTCPTransport creates a new TCP transport.
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		conns:       make(map[NodeID]*connEntry),
		addrs:       make(map[NodeID]string),
		timeout:     5 * time.Second,
		idleTimeout: 30 * time.Second,
	}
}

// NewTCPTransportWithTLS creates a new TCP transport with TLS.
func NewTCPTransportWithTLS(tlsConfig *tls.Config) *TCPTransport {
	t := NewTCPTransport()
	t.tlsConfig = tlsConfig
	return t
}

// LoadTLSConfig loads a TLS config from cert, key, and optional CA file.
func LoadTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load raft TLS keypair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("load raft CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse raft CA file")
		}
		cfg.RootCAs = pool
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// SetAddr sets the network address for a node.
func (t *TCPTransport) SetAddr(nodeID NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Only close existing connection if the address is actually changing
	if currentAddr, ok := t.addrs[nodeID]; ok && currentAddr != addr {
		if entry, ok := t.conns[nodeID]; ok {
			entry.conn.Close()
			delete(t.conns, nodeID)
		}
	}
	t.addrs[nodeID] = addr
}

func (t *TCPTransport) getConn(nodeID NodeID) (net.Conn, error) {
	t.mu.RLock()
	entry, hasEntry := t.conns[nodeID]
	if hasEntry {
		// Check idle timeout
		if time.Since(entry.lastUsed) > t.idleTimeout {
			t.mu.RUnlock()
			t.evictConn(nodeID)
			return t.dialAndStore(nodeID)
		}
		conn := entry.conn
		t.mu.RUnlock()
		return conn, nil
	}
	t.mu.RUnlock()

	return t.dialAndStore(nodeID)
}

func (t *TCPTransport) dialAndStore(nodeID NodeID) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	addr, ok := t.addrs[nodeID]
	if !ok {
		return nil, fmt.Errorf("no address for node %s", nodeID)
	}

	var conn net.Conn
	var err error
	if t.tlsConfig != nil {
		dialer := &net.Dialer{Timeout: t.timeout}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, t.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", addr, t.timeout)
	}
	if err != nil {
		return nil, err
	}
	t.conns[nodeID] = &connEntry{conn: conn, lastUsed: time.Now()}
	return conn, nil
}

// rpcMessage wraps an RPC call over JSON.
type rpcMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (t *TCPTransport) sendRPC(nodeID NodeID, rpcType string, req, resp interface{}) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	msg := rpcMessage{Type: rpcType, Data: data}
	encoded, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	conn, err := t.getConn(nodeID)
	if err != nil {
		return err
	}

	_ = conn.SetWriteDeadline(time.Now().Add(t.timeout))
	if _, err := conn.Write(encoded); err != nil {
		t.invalidateConn(nodeID)
		return err
	}
	// Write newline delimiter
	_, _ = conn.Write([]byte("\n"))

	_ = conn.SetReadDeadline(time.Now().Add(t.timeout))
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(resp); err != nil {
		t.invalidateConn(nodeID)
		return err
	}

	t.updateLastUsed(nodeID)
	return nil
}

func (t *TCPTransport) updateLastUsed(nodeID NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry, ok := t.conns[nodeID]; ok {
		entry.lastUsed = time.Now()
	}
}

func (t *TCPTransport) invalidateConn(nodeID NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry, ok := t.conns[nodeID]; ok {
		entry.conn.Close()
		delete(t.conns, nodeID)
	}
}

func (t *TCPTransport) evictConn(nodeID NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry, ok := t.conns[nodeID]; ok {
		entry.conn.Close()
		delete(t.conns, nodeID)
	}
}

// SendAppendEntries sends an AppendEntries RPC.
func (t *TCPTransport) SendAppendEntries(nodeID NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp := &AppendEntriesResponse{}
	err := t.sendRPC(nodeID, "append_entries", req, resp)
	return resp, err
}

// SendRequestVote sends a RequestVote RPC.
func (t *TCPTransport) SendRequestVote(nodeID NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp := &RequestVoteResponse{}
	err := t.sendRPC(nodeID, "request_vote", req, resp)
	return resp, err
}

// SendInstallSnapshot sends an InstallSnapshot RPC.
func (t *TCPTransport) SendInstallSnapshot(nodeID NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	resp := &InstallSnapshotResponse{}
	err := t.sendRPC(nodeID, "install_snapshot", req, resp)
	return resp, err
}

// ServeRPC serves incoming Raft RPCs on a listener.
func ServeRPC(ln net.Listener, handler RPCHandler) error {
	return serveRPC(ln, handler)
}

// ServeRPCWithTLS serves incoming Raft RPCs on a listener with TLS.
func ServeRPCWithTLS(ln net.Listener, handler RPCHandler, tlsConfig *tls.Config) error {
	if tlsConfig != nil {
		ln = tls.NewListener(ln, tlsConfig)
	}
	return serveRPC(ln, handler)
}

func serveRPC(ln net.Listener, handler RPCHandler) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleRPCConn(conn, handler)
	}
}

func handleRPCConn(conn net.Conn, handler RPCHandler) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	for {
		var msg rpcMessage
		if err := decoder.Decode(&msg); err != nil {
			return
		}
		var resp interface{}
		switch msg.Type {
		case "append_entries":
			var req AppendEntriesRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				continue
			}
			resp = handler.HandleAppendEntries(&req)
		case "request_vote":
			var req RequestVoteRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				continue
			}
			resp = handler.HandleRequestVote(&req)
		case "install_snapshot":
			var req InstallSnapshotRequest
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				continue
			}
			resp = handler.HandleInstallSnapshot(&req)
		default:
			continue
		}
		encoded, _ := json.Marshal(resp)
		_, _ = conn.Write(encoded)
		_, _ = conn.Write([]byte("\n"))
	}
}
