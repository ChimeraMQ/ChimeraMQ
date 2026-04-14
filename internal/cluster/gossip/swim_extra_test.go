package gossip

import (
	"net"
	"testing"
	"time"
)

func newTestSWIM(t *testing.T, nodeID string) *SWIM {
	t.Helper()
	swim, err := NewSWIM(Config{
		NodeID:           NodeID(nodeID),
		BindAddr:         "127.0.0.1",
		BindPort:         0,
		ProbeInterval:    1 * time.Second,
		ProbeTimeout:     500 * time.Millisecond,
		SuspicionTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return swim
}

func TestSWIMMembers(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	members := s.Members()
	if len(members) != 0 {
		t.Errorf("Members() = %d, want 0 (excludes self)", len(members))
	}
}

func TestSWIMAliveCount(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	if s.AliveCount() != 1 {
		t.Errorf("AliveCount = %d, want 1 (self)", s.AliveCount())
	}
}

func TestSWIMMemberList(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	ml := s.MemberList()
	if ml == nil {
		t.Error("MemberList should not be nil")
	}
}

func TestSWIMLocalAddr(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	addr := s.LocalAddr()
	if addr == "" {
		t.Error("LocalAddr should not be empty")
	}
}

func TestSWIMHandleMessagePing(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 1})

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7947")
	msg := &Message{
		Type:        MsgPing,
		SenderID:    "node-2",
		Incarnation: 1,
	}
	s.handleMessage(msg, addr)

	// node-2 should be marked as alive
	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Alive {
		t.Error("node-2 should be alive after ping")
	}
}

func TestSWIMHandleMessageAck(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 1})

	msg := &Message{
		Type:        MsgAck,
		SenderID:    "node-2",
		Incarnation: 2,
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7947")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Alive {
		t.Error("node-2 should be alive after ack")
	}
}

func TestSWIMHandleSuspectOther(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 1})

	msg := &Message{
		Type:        MsgSuspect,
		SenderID:    "node-3",
		TargetID:    "node-2",
		Incarnation: 1,
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Suspect {
		t.Errorf("node-2 should be suspect, got %v", m.State)
	}
}

func TestSWIMHandleSuspectSelf(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	msg := &Message{
		Type:        MsgSuspect,
		SenderID:    "node-2",
		TargetID:    "node-1",
		Incarnation: 1,
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)

	// Local should refute and remain alive
	local := s.MemberList().Local()
	if local.State != Alive {
		t.Errorf("local should remain alive after refuting suspect, got %v", local.State)
	}
	// Incarnation should be higher than the suspect's incarnation
	// handleSuspect calls Refute() which increments incarnation
}

func TestSWIMHandleAlive(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 1})

	msg := &Message{
		Type:        MsgAlive,
		SenderID:    "node-2",
		Incarnation: 2,
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7947")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Alive {
		t.Error("node-2 should be alive after alive msg")
	}
}

func TestSWIMHandleDeadOther(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// Use incarnation 0 so markDead's SetState(id, Dead, 0) takes effect
	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 0})

	msg := &Message{
		Type:     MsgDead,
		SenderID: "node-3",
		TargetID: "node-2",
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Dead {
		t.Errorf("node-2 should be dead, got %v", m)
	}
}

func TestSWIMHandleDeadSelf(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	msg := &Message{
		Type:     MsgDead,
		SenderID: "node-2",
		TargetID: "node-1",
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)

	// Should refute
	local := s.MemberList().Local()
	if local.State != Alive {
		t.Errorf("local should remain alive after refuting dead, got %v", local.State)
	}
}

func TestSWIMHandleSync(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	msg := &Message{
		Type:     MsgSync,
		SenderID: "node-2",
		Members: []MemberMsg{
			{ID: "node-3", Addr: "127.0.0.1", Port: 7948, State: Alive, Incarnation: 1},
			{ID: "node-4", Addr: "127.0.0.1", Port: 7949, State: Alive, Incarnation: 1},
		},
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7947")
	s.handleMessage(msg, addr)

	m3 := s.MemberList().Get("node-3")
	if m3 == nil {
		t.Error("node-3 should be added from sync")
	}
	m4 := s.MemberList().Get("node-4")
	if m4 == nil {
		t.Error("node-4 should be added from sync")
	}
}

func TestSWIMHandlePingReq(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive})

	msg := &Message{
		Type:       MsgPingReq,
		SenderID:   "node-3",
		TargetID:   "node-2",
		TargetAddr: "127.0.0.1:7947",
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)
	// Should forward ping to node-2 — just verify no panic
}

func TestSWIMHandlePingReqNoAddr(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive})

	msg := &Message{
		Type:     MsgPingReq,
		SenderID: "node-3",
		TargetID: "node-2",
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)
	// Should look up node-2's address
}

func TestSWIMHandlePingReqUnknownTarget(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	msg := &Message{
		Type:     MsgPingReq,
		SenderID: "node-3",
		TargetID: "node-unknown",
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)
	// Should return without panic (unknown target)
}

func TestSWIMMergeMemberNew(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.mergeMember(MemberMsg{
		ID: "node-new", Addr: "10.0.0.5", Port: 7946, State: Alive, Incarnation: 1,
	})

	m := s.MemberList().Get("node-new")
	if m == nil {
		t.Error("new member should be added")
	}
}

func TestSWIMMergeMemberUpdate(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 1})

	s.mergeMember(MemberMsg{
		ID: "node-2", State: Suspect, Incarnation: 2,
	})

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Suspect {
		t.Errorf("member should be updated to suspect, got %v", m.State)
	}
}

func TestSWIMMergeMemberOldIncarnation(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 5})

	s.mergeMember(MemberMsg{
		ID: "node-2", State: Dead, Incarnation: 3,
	})

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Alive {
		t.Error("old incarnation should not update state")
	}
}

func TestSWIMCheckSuspicions(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// Use incarnation 0 so markDead's SetState(id, Dead, 0) takes effect
	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 0, LastSeen: time.Now().Add(-10 * time.Second)})

	s.checkSuspicions()

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Dead {
		t.Errorf("suspect member should be dead after timeout, got state=%v", m.State)
	}
}

func TestSWIMCheckSuspicionsNotExpired(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 1, LastSeen: time.Now()})

	s.checkSuspicions()

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Suspect {
		t.Error("recent suspect should not be dead yet")
	}
}

func TestSWIMBroadcastState(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 1})

	// Should not panic — sends to alive members
	s.BroadcastState()
}

func TestSWIMJoinSeed(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// joinSeed just sends a ping — verify no panic with invalid address
	s.joinSeed("127.0.0.1:19999")
}

func TestSWIMProbeNoMembers(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// No other members — should be no-op
	s.probe()
}

func TestSWIMIndirectProbeNoProxies(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 1})

	// No other members to proxy through — should mark suspect
	s.indirectProbe(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, Incarnation: 1})

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Suspect {
		t.Error("node-2 should be suspect when no indirect proxies available")
	}
}

func TestSWIMMarkDead(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// Use incarnation 0 so markDead's SetState(id, Dead, 0) takes effect
	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Suspect, Incarnation: 0})

	s.markDead("node-2")

	m := s.MemberList().Get("node-2")
	if m == nil {
		t.Fatal("node-2 should still exist (removed after 30s)")
	}
	if m.State != Dead {
		t.Errorf("node-2 state = %v, want Dead", m.State)
	}
}

func TestSWIMHandleSuspectOldIncarnation(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 5})

	msg := &Message{
		Type:        MsgSuspect,
		SenderID:    "node-3",
		TargetID:    "node-2",
		Incarnation: 3, // older than current
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7948")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.State != Alive {
		t.Error("old incarnation suspect should not change state")
	}
}

func TestSWIMHandleAliveOldIncarnation(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	s.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive, Incarnation: 5})

	msg := &Message{
		Type:        MsgAlive,
		SenderID:    "node-2",
		Incarnation: 3, // older
	}
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:7947")
	s.handleMessage(msg, addr)

	m := s.MemberList().Get("node-2")
	if m == nil || m.Incarnation != 5 {
		t.Error("old incarnation alive should not downgrade")
	}
}

func TestSWIMAllowedNodesReject(t *testing.T) {
	s, err := NewSWIM(Config{
		NodeID:           NodeID("node-1"),
		BindAddr:         "127.0.0.1",
		BindPort:         0,
		ProbeInterval:    1 * time.Second,
		ProbeTimeout:     500 * time.Millisecond,
		SuspicionTimeout: 5 * time.Second,
		AllowedNodes:     []NodeID{"node-2"}, // only node-2 allowed
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.transport.Close()

	// Try to merge an unknown node
	s.mergeMember(MemberMsg{ID: NodeID("rogue-node"), Addr: "10.0.0.1", Port: 7946, State: Alive, Incarnation: 1})
	if m := s.members.Get("rogue-node"); m != nil {
		t.Error("expected rogue-node to be rejected")
	}

	// Allowed node should be accepted
	s.mergeMember(MemberMsg{ID: NodeID("node-2"), Addr: "10.0.0.2", Port: 7946, State: Alive, Incarnation: 1})
	if m := s.members.Get("node-2"); m == nil {
		t.Error("expected node-2 to be accepted")
	}
}

func TestSWIMAllowedNodesEmpty(t *testing.T) {
	s := newTestSWIM(t, "node-1")
	defer s.transport.Close()

	// No allowlist configured — any node should be accepted
	s.mergeMember(MemberMsg{ID: NodeID("any-node"), Addr: "10.0.0.1", Port: 7946, State: Alive, Incarnation: 1})
	if m := s.members.Get("any-node"); m == nil {
		t.Error("expected any-node to be accepted when no allowlist configured")
	}
}
