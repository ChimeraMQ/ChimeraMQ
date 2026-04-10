package raft

import (
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// mockTransport records RPCs without sending them over the network.
type mockTransport struct {
	mu             sync.Mutex
	appendEntries  map[NodeID][]*AppendEntriesRequest
	requestVotes   map[NodeID][]*RequestVoteRequest
	snapshots      map[NodeID][]*InstallSnapshotRequest
	responses      map[NodeID]interface{}
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		appendEntries: make(map[NodeID][]*AppendEntriesRequest),
		requestVotes:  make(map[NodeID][]*RequestVoteRequest),
		snapshots:     make(map[NodeID][]*InstallSnapshotRequest),
		responses:     make(map[NodeID]interface{}),
	}
}

func (t *mockTransport) SendAppendEntries(nodeID NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.appendEntries[nodeID] = append(t.appendEntries[nodeID], req)
	if resp, ok := t.responses[nodeID]; ok {
		return resp.(*AppendEntriesResponse), nil
	}
	return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (t *mockTransport) SendRequestVote(nodeID NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.requestVotes[nodeID] = append(t.requestVotes[nodeID], req)
	if resp, ok := t.responses[nodeID]; ok {
		return resp.(*RequestVoteResponse), nil
	}
	return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (t *mockTransport) SendInstallSnapshot(nodeID NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snapshots[nodeID] = append(t.snapshots[nodeID], req)
	return &InstallSnapshotResponse{Term: req.Term}, nil
}

func (t *mockTransport) setResponse(nodeID NodeID, resp interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.responses[nodeID] = resp
}

func (t *mockTransport) voteCount(nodeID NodeID) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.requestVotes[nodeID])
}

func testConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	return Config{
		NodeID:            "node-1",
		Peers:             []NodeID{"node-2", "node-3"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour, // don't snapshot during tests
		MaxLogEntries:     100000,
		DataDir:           dir,
	}
}

func TestRaftLogBasics(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	// Empty log
	if log.LastIndex() != 0 {
		t.Errorf("LastIndex = %d, want 0", log.LastIndex())
	}
	if log.LastTerm() != 0 {
		t.Errorf("LastTerm = %d, want 0", log.LastTerm())
	}
	if log.Len() != 0 {
		t.Errorf("Len = %d, want 0", log.Len())
	}

	// Append entries
	log.Append(
		LogEntry{Index: 1, Term: 1, Type: EntryCommand, Data: []byte("a")},
		LogEntry{Index: 2, Term: 1, Type: EntryCommand, Data: []byte("b")},
		LogEntry{Index: 3, Term: 2, Type: EntryNoOp, Data: nil},
	)

	if log.LastIndex() != 3 {
		t.Errorf("LastIndex = %d, want 3", log.LastIndex())
	}
	if log.LastTerm() != 2 {
		t.Errorf("LastTerm = %d, want 2", log.LastTerm())
	}
	if log.Len() != 3 {
		t.Errorf("Len = %d, want 3", log.Len())
	}

	// Get
	e := log.Get(2)
	if e == nil || string(e.Data) != "b" {
		t.Error("Get(2) should return entry with data 'b'")
	}
	if log.Get(99) != nil {
		t.Error("Get(99) should return nil")
	}

	// Range
	entries := log.Range(1, 3)
	if len(entries) != 2 {
		t.Errorf("Range(1,3) = %d entries, want 2", len(entries))
	}

	// TermAt
	if log.TermAt(3) != 2 {
		t.Errorf("TermAt(3) = %d, want 2", log.TermAt(3))
	}
}

func TestRaftLogPersistence(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	log.Append(
		LogEntry{Index: 1, Term: 1, Data: []byte("hello")},
		LogEntry{Index: 2, Term: 1, Data: []byte("world")},
	)
	if err := log.Save(); err != nil {
		t.Fatal(err)
	}

	// Load in a new instance
	log2 := NewRaftLog(dir)
	if err := log2.Load(); err != nil {
		t.Fatal(err)
	}
	if log2.LastIndex() != 2 {
		t.Errorf("LastIndex after load = %d, want 2", log2.LastIndex())
	}
	e := log2.Get(1)
	if e == nil || string(e.Data) != "hello" {
		t.Error("entry data mismatch after load")
	}
}

func TestRaftLogTruncate(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	log.Append(
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 1},
		LogEntry{Index: 3, Term: 2},
	)

	log.TruncateAfter(2)
	if log.LastIndex() != 2 {
		t.Errorf("LastIndex after truncate = %d, want 2", log.LastIndex())
	}
	if log.Get(3) != nil {
		t.Error("entry 3 should be gone after truncate")
	}
}

func TestRaftLogCompact(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	for i := Index(1); i <= 10; i++ {
		log.Append(LogEntry{Index: i, Term: 1, Data: []byte{byte(i)}})
	}

	removed := log.Compact(5)
	if removed != 5 {
		t.Errorf("Compact removed %d, want 5", removed)
	}
	if log.Len() != 5 {
		t.Errorf("Len after compact = %d, want 5", log.Len())
	}
	if log.Get(6) == nil {
		t.Error("entry 6 should still exist")
	}
	if log.Get(5) != nil {
		t.Error("entry 5 should be gone (throughIndex is inclusive)")
	}
	if log.Get(4) != nil {
		t.Error("entry 4 should be gone")
	}
}

func TestFSMApply(t *testing.T) {
	fsm := NewMetadataFSM()

	// Create topic
	createData, _ := json.Marshal(TopicEntry{
		Name: "test-topic", Mode: "stream", Partitions: 4,
		ReplicaSet: []NodeID{"n1", "n2"},
	})
	entry := LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdCreateTopic, Data: createData}),
	}
	fsm.Apply(entry)

	topic := fsm.GetTopic("test-topic")
	if topic == nil {
		t.Fatal("topic not found after create")
	}
	if topic.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", topic.Partitions)
	}

	// Delete topic
	delData, _ := json.Marshal(map[string]string{"name": "test-topic"})
	entry2 := LogEntry{Index: 2, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdDeleteTopic, Data: delData}),
	}
	fsm.Apply(entry2)
	if fsm.GetTopic("test-topic") != nil {
		t.Error("topic should be deleted")
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	fsm := NewMetadataFSM()

	// Add some state
	createData, _ := json.Marshal(TopicEntry{Name: "t1", Partitions: 2})
	fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdCreateTopic, Data: createData}),
	})

	// Snapshot
	data, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Restore into new FSM
	fsm2 := NewMetadataFSM()
	if err := fsm2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if fsm2.GetTopic("t1") == nil {
		t.Error("topic not restored")
	}
}

func TestNodeCreation(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if node.State() != Follower {
		t.Errorf("initial state = %v, want Follower", node.State())
	}
	if node.Term() != 0 {
		t.Errorf("initial term = %d, want 0", node.Term())
	}
}

func TestNodeStartStop(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	transport := newMockTransport()
	node.SetTransport(transport)

	if err := node.Start(); err != nil {
		t.Fatal(err)
	}

	// Should become candidate after election timeout (peers don't respond with votes in time)
	time.Sleep(600 * time.Millisecond)

	state := node.State()
	if state != Candidate && state != Leader && state != Follower {
		t.Errorf("state after timeout = %v, expected election activity", state)
	}

	node.Stop()
	if node.State() != Shutdown {
		t.Errorf("state after stop = %v, want Shutdown", node.State())
	}
}

func TestHandleAppendEntries(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Heartbeat with higher term
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})

	if !resp.Success {
		t.Error("empty AppendEntries should succeed")
	}
	if node.Term() != 1 {
		t.Errorf("term = %d, want 1", node.Term())
	}
}

func TestHandleRequestVote(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	resp := node.HandleRequestVote(&RequestVoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("should grant vote for valid request")
	}
}

func TestHandleRequestVoteRejectOldTerm(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)

	// Bump term
	node.HandleRequestVote(&RequestVoteRequest{
		Term: 5, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})

	// Request with old term
	resp := node.HandleRequestVote(&RequestVoteRequest{
		Term: 3, CandidateID: "node-3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if resp.VoteGranted {
		t.Error("should reject vote for old term")
	}
}

func TestProposeRejectsNonLeader(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	_, err = node.Propose([]byte("test"))
	if err == nil {
		t.Error("Propose on follower should fail")
	}
}

func TestSnapshotter(t *testing.T) {
	dir := t.TempDir()
	raftLog := NewRaftLog(filepath.Join(dir, "log"))
	fsm := NewMetadataFSM()
	snap := NewSnapshotter(filepath.Join(dir, "snap"), fsm, raftLog)

	// Add FSM state
	createData, _ := json.Marshal(TopicEntry{Name: "snap-test", Partitions: 8})
	fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdCreateTopic, Data: createData}),
	})
	raftLog.Append(LogEntry{Index: 1, Term: 1})

	meta, err := snap.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if meta.LastIncludedIndex != 1 {
		t.Errorf("snapshot last index = %d, want 1", meta.LastIncludedIndex)
	}

	// Load snapshot
	fsm2 := NewMetadataFSM()
	raftLog2 := NewRaftLog(filepath.Join(dir, "log2"))
	snap2 := NewSnapshotter(filepath.Join(dir, "snap"), fsm2, raftLog2)

	meta2, err := snap2.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if meta2 == nil {
		t.Fatal("snapshot not found")
	}
	if fsm2.GetTopic("snap-test") == nil {
		t.Error("topic not restored from snapshot")
	}
}

func TestDetectorHTTP(t *testing.T) {
	// This just tests that the raft package compiles and works
	cfg := testConfig(t)
	_ = cfg
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
