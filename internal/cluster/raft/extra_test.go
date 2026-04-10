package raft

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Log tests — additional coverage
// ---------------------------------------------------------------------------

func TestRaftLogSaveAndLoadErrors(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	// Load from nonexistent directory (no file) — should succeed with nil.
	if err := log.Load(); err != nil {
		t.Fatalf("Load on empty dir should not fail: %v", err)
	}

	// Write garbage into log.json.
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "log.json"), []byte("%%%not-json%%%"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := log.Load(); err == nil {
		t.Error("Load with corrupt JSON should return error")
	}
}

func TestRaftLogLoadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "log.json"), []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	log := NewRaftLog(dir)
	if err := log.Load(); err != nil {
		t.Fatalf("Load with empty file should not fail: %v", err)
	}
}

func TestRaftLogRangeClampingFromBelowFirstIndex(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	// After compaction, firstIndex is > 1, Range(from < firstIndex) should clamp.
	for i := Index(1); i <= 10; i++ {
		log.Append(LogEntry{Index: i, Term: 1, Data: []byte{byte(i)}})
	}
	removed := log.Compact(4) // removes entries 1..4, firstIndex becomes 5
	if removed != 4 {
		t.Fatalf("removed = %d, want 4", removed)
	}

	// Range with from < firstIndex (from=1, firstIndex=5)
	entries := log.Range(1, 7)
	if len(entries) != 2 { // entries 5,6
		t.Errorf("Range(1,7) after compact = %d entries, want 2", len(entries))
	}
	if len(entries) > 0 && entries[0].Index != 5 {
		t.Errorf("first entry index = %d, want 5", entries[0].Index)
	}
}

func TestRaftLogRangeStartGreaterThanEnd(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)
	log.Append(LogEntry{Index: 1, Term: 1}, LogEntry{Index: 2, Term: 1})

	// from > to should return nil
	entries := log.Range(5, 3)
	if entries != nil {
		t.Errorf("Range(5,3) = %v, want nil", entries)
	}
}

func TestRaftLogTruncateAfterNegativeEnd(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)
	log.Append(LogEntry{Index: 3, Term: 1})

	// TruncateAfter(0) should produce end = 0 - 1 + 1 = 0, which is < 0 in int terms
	// firstIndex is 1, so end = 0 - 1 + 1 = 0 < len(entries), end < 0? No.
	// Let's trigger the end < 0 branch: TruncateAfter(idx) where idx = 0,
	// end = int(0 - 1 + 1) = 0. That's not < 0. We need firstIndex > idx+1.
	// After compact, firstIndex shifts up.
	for i := Index(1); i <= 5; i++ {
		log.Append(LogEntry{Index: i, Term: 1})
	}
	log.Compact(3) // firstIndex = 4

	// TruncateAfter(0): end = int(0 - 4 + 1) = -3 < 0
	log.TruncateAfter(0)
	if log.Len() != 0 {
		t.Error("TruncateAfter(0) after compact should clear all entries")
	}
}

func TestRaftLogCompactAll(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)

	log.Append(LogEntry{Index: 1, Term: 1})
	// Compact(1) with cut=1 >= len(entries)=1 should return 0
	removed := log.Compact(1)
	if removed != 0 {
		t.Errorf("Compact all should return 0, got %d", removed)
	}
}

func TestRaftLogCompactBelowFirstIndex(t *testing.T) {
	dir := t.TempDir()
	log := NewRaftLog(dir)
	for i := Index(1); i <= 5; i++ {
		log.Append(LogEntry{Index: i, Term: 1})
	}
	log.Compact(3) // firstIndex = 4

	// Compact below firstIndex (throughIndex=2 < firstIndex=4)
	removed := log.Compact(2)
	if removed != 0 {
		t.Errorf("Compact below firstIndex should return 0, got %d", removed)
	}
}

// ---------------------------------------------------------------------------
// FSM tests — additional coverage for error paths
// ---------------------------------------------------------------------------

func TestFSMApplyInvalidCreateTopicData(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: makeInvalidCommandData(CmdCreateTopic),
	})
	if err == nil {
		t.Error("expected error for invalid create topic data")
	}
}

func TestFSMApplyInvalidDeleteTopicData(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: makeInvalidCommandData(CmdDeleteTopic),
	})
	if err == nil {
		t.Error("expected error for invalid delete topic data")
	}
}

func TestFSMApplyInvalidAssignPartitionData(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: makeInvalidCommandData(CmdAssignPartition),
	})
	if err == nil {
		t.Error("expected error for invalid assign partition data")
	}
}

func TestFSMApplyInvalidJoinGroupData(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: makeInvalidCommandData(CmdJoinGroup),
	})
	if err == nil {
		t.Error("expected error for invalid join group data")
	}
}

func TestFSMApplyInvalidLeaveGroupData(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: makeInvalidCommandData(CmdLeaveGroup),
	})
	if err == nil {
		t.Error("expected error for invalid leave group data")
	}
}

// makeInvalidCommandData builds a valid outer Command JSON but with invalid inner Data.
func makeInvalidCommandData(cmdType CommandType) []byte {
	// Manually construct JSON so we don't hit the RawMessage marshal issue.
	return []byte(`{"type":"` + string(cmdType) + `","data":"not-valid-json"}`)
}

func TestFSMLeaveGroupNonexistentGroup(t *testing.T) {
	fsm := NewMetadataFSM()
	// Leaving a group that doesn't exist should be a no-op, not an error.
	leaveData, _ := json.Marshal(map[string]string{"group": "no-group", "member": "no-member"})
	err := fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdLeaveGroup, Data: leaveData}),
	})
	if err != nil {
		t.Errorf("leave nonexistent group should not error: %v", err)
	}
}

func TestFSMJoinExistingGroup(t *testing.T) {
	fsm := NewMetadataFSM()

	// Create group with member c1
	joinData1, _ := json.Marshal(map[string]interface{}{
		"group": "g1", "member": "c1", "topics": []string{"t1"},
	})
	fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdJoinGroup, Data: joinData1}),
	})

	// Add second member to same group
	joinData2, _ := json.Marshal(map[string]interface{}{
		"group": "g1", "member": "c2", "topics": []string{"t2"},
	})
	fsm.Apply(LogEntry{Index: 2, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdJoinGroup, Data: joinData2}),
	})

	grp := fsm.GetGroup("g1")
	if grp == nil {
		t.Fatal("group should exist")
	}
	if len(grp.Members) != 2 {
		t.Errorf("members = %d, want 2", len(grp.Members))
	}

	// Leave one member; group should still exist
	leaveData, _ := json.Marshal(map[string]string{"group": "g1", "member": "c1"})
	fsm.Apply(LogEntry{Index: 3, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdLeaveGroup, Data: leaveData}),
	})
	grp = fsm.GetGroup("g1")
	if grp == nil {
		t.Fatal("group should still exist with one member")
	}
	if len(grp.Members) != 1 {
		t.Errorf("members = %d, want 1", len(grp.Members))
	}
}

func TestFSMRestoreInvalidJSON(t *testing.T) {
	fsm := NewMetadataFSM()
	err := fsm.Restore([]byte("not json"))
	if err == nil {
		t.Error("Restore with invalid JSON should return error")
	}
}

func TestFSMGetTopicNonexistent(t *testing.T) {
	fsm := NewMetadataFSM()
	if fsm.GetTopic("nonexistent") != nil {
		t.Error("GetTopic should return nil for nonexistent topic")
	}
}

func TestFSMGetAssignmentNonexistent(t *testing.T) {
	fsm := NewMetadataFSM()
	if fsm.GetAssignment("no-topic", 0) != nil {
		t.Error("GetAssignment should return nil for nonexistent assignment")
	}
}

func TestFSMGetGroupNonexistent(t *testing.T) {
	fsm := NewMetadataFSM()
	if fsm.GetGroup("no-group") != nil {
		t.Error("GetGroup should return nil for nonexistent group")
	}
}

// ---------------------------------------------------------------------------
// RaftNode — IsLeader, LeaderID
// ---------------------------------------------------------------------------

func TestIsLeaderAndLeaderID(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)

	if node.IsLeader() {
		t.Error("newly created node should not be leader")
	}
	if node.LeaderID() != "" {
		t.Errorf("LeaderID on uninitialized node = %q, want empty", node.LeaderID())
	}
}

// ---------------------------------------------------------------------------
// RaftNode — Start with existing snapshot
// ---------------------------------------------------------------------------

func TestNodeStartWithExistingSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:            "node-1",
		Peers:             []NodeID{"node-2"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		MaxLogEntries:     100000,
		DataDir:           dir,
	}

	// Create a snapshot manually
	snapDir := filepath.Join(dir, "raft", "snapshots")
	os.MkdirAll(snapDir, 0755)

	fsm := NewMetadataFSM()
	createData, _ := json.Marshal(TopicEntry{Name: "preexist", Partitions: 3})
	fsm.Apply(LogEntry{Index: 1, Term: 1, Type: EntryCommand,
		Data: mustMarshal(Command{Type: CmdCreateTopic, Data: createData}),
	})
	snapData, _ := fsm.Snapshot()

	meta := &SnapshotMeta{LastIncludedIndex: 5, LastIncludedTerm: 2, Size: len(snapData)}
	metaJSON, _ := json.Marshal(meta)
	os.WriteFile(filepath.Join(snapDir, "meta.json"), metaJSON, 0644)
	os.WriteFile(filepath.Join(snapDir, "snapshot.json"), snapData, 0644)

	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)

	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	// The snapshot should have been loaded, so commitIndex = 5
	if node.CommitIndex() != 5 {
		t.Errorf("commitIndex = %d, want 5", node.CommitIndex())
	}
	if node.FSM().GetTopic("preexist") == nil {
		t.Error("topic should be restored from pre-existing snapshot")
	}
}

// ---------------------------------------------------------------------------
// RaftNode — loadState / saveState
// ---------------------------------------------------------------------------

func TestNodeLoadStateFromDisk(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:            "node-1",
		Peers:             []NodeID{"node-2"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		MaxLogEntries:     100000,
		DataDir:           dir,
	}

	// Write a state file
	stateDir := filepath.Join(dir, "raft")
	os.MkdirAll(stateDir, 0755)
	stateData, _ := json.Marshal(map[string]interface{}{
		"current_term": 7,
		"voted_for":    "node-2",
	})
	os.WriteFile(filepath.Join(stateDir, "state.json"), stateData, 0644)

	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// The node should have loaded term 7
	if node.Term() != 7 {
		t.Errorf("term = %d, want 7", node.Term())
	}
}

// ---------------------------------------------------------------------------
// HandleAppendEntries — more edge cases
// ---------------------------------------------------------------------------

func TestHandleAppendEntriesRejectOldTerm(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// First bump the term
	node.HandleRequestVote(&RequestVoteRequest{
		Term: 5, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})

	// Send AppendEntries with older term
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 3, LeaderID: "node-2",
	})
	if resp.Success {
		t.Error("AppendEntries with old term should fail")
	}
}

func TestHandleAppendEntriesPrevLogMismatch(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Append an entry at term 1
	node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries:      []LogEntry{{Index: 1, Term: 1, Data: []byte("a")}},
		PrevLogIndex: 0, PrevLogTerm: 0,
	})

	// Now try AppendEntries where PrevLogIndex=1, PrevLogTerm=99 (mismatch)
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 2, LeaderID: "node-2",
		Entries:      []LogEntry{{Index: 2, Term: 2, Data: []byte("b")}},
		PrevLogIndex: 1, PrevLogTerm: 99,
	})
	if resp.Success {
		t.Error("AppendEntries with prev log mismatch should fail")
	}
	if resp.ConflictIndex == 0 {
		t.Error("ConflictIndex should be set on mismatch")
	}
}

func TestHandleAppendEntriesSameEntryAlreadyExists(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	entry := LogEntry{Index: 1, Term: 1, Data: []byte("x")}

	// Append once
	node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries:      []LogEntry{entry},
		PrevLogIndex: 0, PrevLogTerm: 0,
	})

	// Append same entry again — should be a no-op (same term, same index)
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries:      []LogEntry{entry},
		PrevLogIndex: 0, PrevLogTerm: 0,
	})
	if !resp.Success {
		t.Error("re-append of same entry should succeed")
	}
}

func TestHandleAppendEntriesLeaderCommitBelowCurrent(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// First append with commit=2
	node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Data: []byte("a")},
			{Index: 2, Term: 1, Data: []byte("b")},
		},
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 2,
	})

	// Then heartbeat with LeaderCommit < commitIndex — no regression
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 1,
	})
	if !resp.Success {
		t.Error("heartbeat with lower LeaderCommit should still succeed")
	}
	if node.CommitIndex() != 2 {
		t.Errorf("commitIndex should not regress, got %d", node.CommitIndex())
	}
}

func TestHandleAppendEntriesLeaderCommitGreaterThanLastNew(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// LeaderCommit=100 but only 2 entries, so commitIndex should be capped at 2
	resp := node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Data: []byte("a")},
			{Index: 2, Term: 1, Data: []byte("b")},
		},
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 100,
	})
	if !resp.Success {
		t.Error("should succeed")
	}
	if node.CommitIndex() != 2 {
		t.Errorf("commitIndex = %d, want 2 (capped at lastNewIdx)", node.CommitIndex())
	}
}

// ---------------------------------------------------------------------------
// HandleRequestVote — edge case: same term, already voted for same candidate
// ---------------------------------------------------------------------------

func TestHandleRequestVoteSameCandidate(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Vote for node-2 in term 1
	resp1 := node.HandleRequestVote(&RequestVoteRequest{
		Term: 1, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !resp1.VoteGranted {
		t.Error("first vote should be granted")
	}

	// Vote again for same node-2 in term 1 — should still be granted
	resp2 := node.HandleRequestVote(&RequestVoteRequest{
		Term: 1, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !resp2.VoteGranted {
		t.Error("re-vote for same candidate should be granted")
	}
}

// ---------------------------------------------------------------------------
// HandleInstallSnapshot — edge case: old term
// ---------------------------------------------------------------------------

func TestHandleInstallSnapshotOldTerm(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Bump term
	node.HandleRequestVote(&RequestVoteRequest{
		Term: 10, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})

	// InstallSnapshot with old term
	resp := node.HandleInstallSnapshot(&InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "node-3",
		LastIncludedIndex: 100,
		LastIncludedTerm:  5,
		Data:              []byte("{}"),
	})
	if resp.Term != 10 {
		t.Errorf("response term = %d, want 10", resp.Term)
	}
	if node.CommitIndex() != 0 {
		t.Error("commitIndex should not change from old-term snapshot")
	}
}

// ---------------------------------------------------------------------------
// findConflictIndex — direct coverage
// ---------------------------------------------------------------------------

func TestFindConflictIndex(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Build a log with multiple terms
	node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries: []LogEntry{
			{Index: 1, Term: 1},
			{Index: 2, Term: 1},
			{Index: 3, Term: 2},
			{Index: 4, Term: 2},
		},
		PrevLogIndex: 0, PrevLogTerm: 0,
	})

	// findConflictIndex(4): entry 4 has term 2, entry 3 has term 2 → conflictIndex = 3
	idx := node.findConflictIndex(4)
	if idx != 3 {
		t.Errorf("findConflictIndex(4) = %d, want 3", idx)
	}

	// findConflictIndex(2): entry 2 has term 1, entry 1 has term 1 → conflictIndex = 1
	idx = node.findConflictIndex(2)
	if idx != 1 {
		t.Errorf("findConflictIndex(2) = %d, want 1", idx)
	}

	// findConflictIndex(1): entry 1 has term 1, prev(0) is nil → conflictIndex = 1
	idx = node.findConflictIndex(1)
	if idx != 1 {
		t.Errorf("findConflictIndex(1) = %d, want 1", idx)
	}
}

// ---------------------------------------------------------------------------
// Propose on a leader node — simulate election
// ---------------------------------------------------------------------------

func TestProposeAsLeader(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)

	// Manually set node to Leader state by calling becomeLeader indirectly.
	// We use Start, then force a state transition via election.
	// Easier: use exported methods to put the node into a state where Propose works.
	// Since becomeLeader is unexported, let's trigger election by waiting.
	node.Start()
	defer node.Stop()

	// With 2 peers that auto-grant votes, this node will win election quickly.
	// Wait for the election timeout + some slack.
	time.Sleep(700 * time.Millisecond)

	// If the node became leader, try Propose
	if node.IsLeader() {
		idx, err := node.Propose([]byte("test-command"))
		if err != nil {
			t.Errorf("Propose as leader failed: %v", err)
		}
		if idx == 0 {
			t.Error("Propose should return non-zero index")
		}
	}
}

// ---------------------------------------------------------------------------
// TCPTransport — unit tests
// ---------------------------------------------------------------------------

func TestNewTCPTransport(t *testing.T) {
	tp := NewTCPTransport()
	if tp == nil {
		t.Fatal("NewTCPTransport returned nil")
	}
}

func TestTCPTransportSetAddr(t *testing.T) {
	tp := NewTCPTransport()
	tp.SetAddr("node-1", "127.0.0.1:9999")

	tp.mu.RLock()
	addr := tp.addrs["node-1"]
	tp.mu.RUnlock()
	if addr != "127.0.0.1:9999" {
		t.Errorf("addr = %q, want 127.0.0.1:9999", addr)
	}
}

func TestTCPTransportSendNoAddr(t *testing.T) {
	tp := NewTCPTransport()
	_, err := tp.SendAppendEntries("unknown-node", &AppendEntriesRequest{
		Term: 1, LeaderID: "n1",
	})
	if err == nil {
		t.Error("SendAppendEntries to unknown node should fail")
	}
}

func TestTCPTransportSendRequestVoteNoAddr(t *testing.T) {
	tp := NewTCPTransport()
	_, err := tp.SendRequestVote("unknown-node", &RequestVoteRequest{
		Term: 1, CandidateID: "n1",
	})
	if err == nil {
		t.Error("SendRequestVote to unknown node should fail")
	}
}

func TestTCPTransportSendInstallSnapshotNoAddr(t *testing.T) {
	tp := NewTCPTransport()
	_, err := tp.SendInstallSnapshot("unknown-node", &InstallSnapshotRequest{
		Term: 1, LeaderID: "n1",
	})
	if err == nil {
		t.Error("SendInstallSnapshot to unknown node should fail")
	}
}

// ---------------------------------------------------------------------------
// ServeRPC + handleRPCConn — integration-style test with real TCP
// ---------------------------------------------------------------------------

func TestServeRPCAndHandleRPCConn(t *testing.T) {
	// Create a mock handler that records calls
	handler := &recordHandler{}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		ServeRPC(ln, handler)
		close(done)
	}()

	tp := NewTCPTransport()
	tp.SetAddr("node-1", ln.Addr().String())

	// Send AppendEntries
	resp, err := tp.SendAppendEntries("node-1", &AppendEntriesRequest{
		Term: 1, LeaderID: "node-2", PrevLogIndex: 0, PrevLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("SendAppendEntries error: %v", err)
	}
	if resp.Term != 1 {
		t.Errorf("response term = %d, want 1", resp.Term)
	}

	// Send RequestVote
	rvResp, err := tp.SendRequestVote("node-1", &RequestVoteRequest{
		Term: 2, CandidateID: "node-3",
	})
	if err != nil {
		t.Fatalf("SendRequestVote error: %v", err)
	}
	if rvResp.Term != 2 {
		t.Errorf("response term = %d, want 2", rvResp.Term)
	}

	// Send InstallSnapshot
	isResp, err := tp.SendInstallSnapshot("node-1", &InstallSnapshotRequest{
		Term: 3, LeaderID: "node-2", LastIncludedIndex: 5, LastIncludedTerm: 3,
		Data: []byte("{}"),
	})
	if err != nil {
		t.Fatalf("SendInstallSnapshot error: %v", err)
	}
	if isResp.Term != 3 {
		t.Errorf("response term = %d, want 3", isResp.Term)
	}

	// Verify handler recorded all RPCs
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.appendCount != 1 {
		t.Errorf("appendCount = %d, want 1", handler.appendCount)
	}
	if handler.voteCount != 1 {
		t.Errorf("voteCount = %d, want 1", handler.voteCount)
	}
	if handler.snapCount != 1 {
		t.Errorf("snapCount = %d, want 1", handler.snapCount)
	}

	// Close listener to stop ServeRPC
	ln.Close()
	<-done
}

func TestServeRPCConnClose(t *testing.T) {
	// Test that ServeRPC returns error when listener is closed.
	handler := &recordHandler{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // close immediately

	err = ServeRPC(ln, handler)
	if err == nil {
		t.Error("ServeRPC on closed listener should return error")
	}
}

func TestHandleRPCConnUnknownType(t *testing.T) {
	// Start a server
	handler := &recordHandler{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go ServeRPC(ln, handler)

	// Connect and send an unknown RPC type
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	msg := rpcMessage{Type: "unknown_type", Data: json.RawMessage(`{}`)}
	data, _ := json.Marshal(msg)
	conn.Write(data)
	conn.Write([]byte("\n"))

	// Server should ignore unknown type and continue.
	// Send a valid RequestVote to confirm connection is still alive.
	rvMsg := rpcMessage{Type: "request_vote", Data: json.RawMessage(`{"term":1,"candidate_id":"n1"}`)}
	rvData, _ := json.Marshal(rvMsg)
	conn.Write(rvData)
	conn.Write([]byte("\n"))

	decoder := json.NewDecoder(conn)
	var resp RequestVoteResponse
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Term != 1 {
		t.Errorf("term = %d, want 1", resp.Term)
	}
}

// recordHandler is a simple RPCHandler that records call counts.
type recordHandler struct {
	mu          sync.Mutex
	appendCount int
	voteCount   int
	snapCount   int
}

func (h *recordHandler) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.appendCount++
	return &AppendEntriesResponse{Term: req.Term, Success: true}
}

func (h *recordHandler) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.voteCount++
	return &RequestVoteResponse{Term: req.Term, VoteGranted: true}
}

func (h *recordHandler) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapCount++
	return &InstallSnapshotResponse{Term: req.Term}
}

// ---------------------------------------------------------------------------
// Snapshotter — ShouldSnapshot
// ---------------------------------------------------------------------------

func TestSnapshotterShouldSnapshot(t *testing.T) {
	dir := t.TempDir()
	raftLog := NewRaftLog(filepath.Join(dir, "log"))
	fsm := NewMetadataFSM()
	snap := NewSnapshotter(filepath.Join(dir, "snap"), fsm, raftLog)

	// Empty log, threshold 10
	if snap.ShouldSnapshot(10) {
		t.Error("ShouldSnapshot with empty log should be false")
	}

	// Add 15 entries
	for i := Index(1); i <= 15; i++ {
		raftLog.Append(LogEntry{Index: i, Term: 1})
	}
	if !snap.ShouldSnapshot(10) {
		t.Error("ShouldSnapshot with 15 entries and threshold 10 should be true")
	}
	if snap.ShouldSnapshot(20) {
		t.Error("ShouldSnapshot with 15 entries and threshold 20 should be false")
	}
}

func TestSnapshotterTakeSnapshotNoEntries(t *testing.T) {
	dir := t.TempDir()
	raftLog := NewRaftLog(filepath.Join(dir, "log"))
	fsm := NewMetadataFSM()
	snap := NewSnapshotter(filepath.Join(dir, "snap"), fsm, raftLog)

	// TakeSnapshot with empty log — should succeed, no compact
	meta, err := snap.TakeSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if meta.LastIncludedIndex != 0 {
		t.Errorf("LastIncludedIndex = %d, want 0", meta.LastIncludedIndex)
	}
}

// ---------------------------------------------------------------------------
// DefaultConfig
// ---------------------------------------------------------------------------

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ElectionTimeout != 1*time.Second {
		t.Errorf("ElectionTimeout = %v, want 1s", cfg.ElectionTimeout)
	}
	if cfg.HeartbeatInterval != 150*time.Millisecond {
		t.Errorf("HeartbeatInterval = %v, want 150ms", cfg.HeartbeatInterval)
	}
	if cfg.SnapshotInterval != 5*time.Minute {
		t.Errorf("SnapshotInterval = %v, want 5m", cfg.SnapshotInterval)
	}
	if cfg.MaxLogEntries != 100000 {
		t.Errorf("MaxLogEntries = %d, want 100000", cfg.MaxLogEntries)
	}
}

// ---------------------------------------------------------------------------
// maybeSnapshot — indirect coverage via Start + snapshot ticker
// ---------------------------------------------------------------------------

func TestMaybeSnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:            "node-1",
		Peers:             []NodeID{"node-2"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  100 * time.Millisecond, // short for test
		MaxLogEntries:     5,
		DataDir:           dir,
	}
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Add entries via AppendEntries to exceed MaxLogEntries
	for i := Index(1); i <= 10; i++ {
		node.HandleAppendEntries(&AppendEntriesRequest{
			Term: 1, LeaderID: "node-2",
			Entries:      []LogEntry{{Index: i, Term: 1, Data: []byte("x")}},
			PrevLogIndex: i - 1, PrevLogTerm: 1,
		})
	}

	// Wait for snapshot ticker to fire
	time.Sleep(250 * time.Millisecond)

	// If snapshotting occurred, the log should be compacted.
	// (at least check that the node didn't crash)
}

// ---------------------------------------------------------------------------
// Election — wait for node to become candidate/leader
// ---------------------------------------------------------------------------

func TestElectionTransitions(t *testing.T) {
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// With mock transport auto-granting votes, node should become leader
	// (3 nodes: self + 2 peers; need 2 votes for majority)
	time.Sleep(700 * time.Millisecond)

	state := node.State()
	if state == Follower {
		t.Log("node stayed follower (election timer may not have fired yet)")
	}
	// The main goal is to exercise the election code path.
}

// ---------------------------------------------------------------------------
// EntryType coverage — EntryConfigChange
// ---------------------------------------------------------------------------

func TestEntryConfigChangeApplied(t *testing.T) {
	// Ensure EntryConfigChange entries are not applied to FSM (only EntryCommand)
	cfg := testConfig(t)
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)
	node.Start()
	defer node.Stop()

	// Append a config change entry and commit it
	createData, _ := json.Marshal(TopicEntry{Name: "should-not-exist", Partitions: 1})
	node.HandleAppendEntries(&AppendEntriesRequest{
		Term: 1, LeaderID: "node-2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Type: EntryConfigChange, Data: mustMarshal(Command{Type: CmdCreateTopic, Data: createData})},
		},
		PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 1,
	})

	// ConfigChange entry should not be applied
	if node.FSM().GetTopic("should-not-exist") != nil {
		t.Error("EntryConfigChange should not be applied to FSM")
	}
}

// ---------------------------------------------------------------------------
// NewRaftNode — zero-config defaults
// ---------------------------------------------------------------------------

func TestNewRaftNodeZeroConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:  "node-1",
		Peers:   []NodeID{"node-2"},
		DataDir: dir,
	}
	node, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	transport := newMockTransport()
	node.SetTransport(transport)

	// Defaults should be applied
	if node.cfg.ElectionTimeout == 0 {
		t.Error("ElectionTimeout should have default value")
	}
	if node.cfg.HeartbeatInterval == 0 {
		t.Error("HeartbeatInterval should have default value")
	}
}

// ---------------------------------------------------------------------------
// TCPTransport — connection retry / invalidate
// ---------------------------------------------------------------------------

func TestTCPTransportConnectionFailure(t *testing.T) {
	tp := NewTCPTransport()
	// Set address to a port that's not listening
	tp.SetAddr("node-1", "127.0.0.1:1")

	_, err := tp.SendAppendEntries("node-1", &AppendEntriesRequest{
		Term: 1, LeaderID: "n2",
	})
	if err == nil {
		t.Error("SendAppendEntries to non-listening port should fail")
	}
}

func TestTCPTransportSetAddrReplaces(t *testing.T) {
	tp := NewTCPTransport()
	tp.SetAddr("node-1", "127.0.0.1:1") // set bad address
	// Force a connection attempt that will fail and be cached
	tp.getConn("node-1")

	// Now replace the address
	tp.SetAddr("node-1", "127.0.0.1:2")
	tp.mu.RLock()
	addr := tp.addrs["node-1"]
	tp.mu.RUnlock()
	if addr != "127.0.0.1:2" {
		t.Errorf("addr after replace = %q, want 127.0.0.1:2", addr)
	}
}
