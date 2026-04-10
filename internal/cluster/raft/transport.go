package raft

import (
	"encoding/json"
	"fmt"
	"net"
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

// TCPTransport implements Transport over TCP.
type TCPTransport struct {
	mu      sync.RWMutex
	conns   map[NodeID]net.Conn
	addrs   map[NodeID]string
	timeout time.Duration
}

// NewTCPTransport creates a new TCP transport.
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{
		conns:   make(map[NodeID]net.Conn),
		addrs:   make(map[NodeID]string),
		timeout: 5 * time.Second,
	}
}

// SetAddr sets the network address for a node.
func (t *TCPTransport) SetAddr(nodeID NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Close existing connection if any
	if conn, ok := t.conns[nodeID]; ok {
		conn.Close()
		delete(t.conns, nodeID)
	}
	t.addrs[nodeID] = addr
}

func (t *TCPTransport) getConn(nodeID NodeID) (net.Conn, error) {
	t.mu.RLock()
	if conn, ok := t.conns[nodeID]; ok {
		t.mu.RUnlock()
		return conn, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	addr, ok := t.addrs[nodeID]
	if !ok {
		return nil, fmt.Errorf("no address for node %s", nodeID)
	}

	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return nil, err
	}
	t.conns[nodeID] = conn
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
	conn.Write([]byte("\n"))

	conn.SetReadDeadline(time.Now().Add(t.timeout))
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(resp); err != nil {
		t.invalidateConn(nodeID)
		return err
	}
	return nil
}

func (t *TCPTransport) invalidateConn(nodeID NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[nodeID]; ok {
		conn.Close()
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
			json.Unmarshal(msg.Data, &req)
			resp = handler.HandleAppendEntries(&req)
		case "request_vote":
			var req RequestVoteRequest
			json.Unmarshal(msg.Data, &req)
			resp = handler.HandleRequestVote(&req)
		case "install_snapshot":
			var req InstallSnapshotRequest
			json.Unmarshal(msg.Data, &req)
			resp = handler.HandleInstallSnapshot(&req)
		default:
			continue
		}
		encoded, _ := json.Marshal(resp)
		conn.Write(encoded)
		conn.Write([]byte("\n"))
	}
}
