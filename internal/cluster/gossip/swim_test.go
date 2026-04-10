package gossip

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func extractPort(addr string) int {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return 0
	}
	p, _ := strconv.Atoi(parts[1])
	return p
}

func encodeMessage(msg *Message) ([]byte, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	return buf, nil
}

func decodeMessage(buf []byte) (*Message, error) {
	if len(buf) < 4 {
		return nil, nil
	}
	length := int(binary.BigEndian.Uint32(buf[:4]))
	var msg Message
	err := json.Unmarshal(buf[4 : 4+length], &msg)
	return &msg, err
}

func TestMemberList(t *testing.T) {
	ml := NewMemberList("node-1")

	ml.Add(&Member{ID: "node-1", Addr: "127.0.0.1", Port: 7946, State: Alive})
	ml.Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive})
	ml.Add(&Member{ID: "node-3", Addr: "127.0.0.1", Port: 7948, State: Alive})

	if ml.Size() != 3 {
		t.Errorf("Size = %d, want 3", ml.Size())
	}

	all := ml.All()
	if len(all) != 2 { // excludes self
		t.Errorf("All() = %d members, want 2 (excludes self)", len(all))
	}

	alive := ml.AliveMembers()
	if len(alive) != 2 {
		t.Errorf("AliveMembers() = %d, want 2", len(alive))
	}

	// State change
	ml.SetState("node-2", Suspect, 0)
	m := ml.Get("node-2")
	if m.State != Suspect {
		t.Errorf("node-2 state = %v, want Suspect", m.State)
	}

	// Remove
	ml.Remove("node-3")
	if ml.Get("node-3") != nil {
		t.Error("node-3 should be removed")
	}
}

func TestMemberListSelectRandom(t *testing.T) {
	ml := NewMemberList("node-1")
	ml.Add(&Member{ID: "node-1", Addr: "127.0.0.1", Port: 7946, State: Alive})
	ml.Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: 7947, State: Alive})
	ml.Add(&Member{ID: "node-3", Addr: "127.0.0.1", Port: 7948, State: Alive})

	selected := ml.SelectRandom(nil)
	if selected == nil {
		t.Fatal("SelectRandom should return a member")
	}
	if selected.ID == "node-1" {
		t.Error("SelectRandom should not return self")
	}

	// Exclude all except one
	exclude := map[NodeID]bool{"node-2": true}
	selected = ml.SelectRandom(exclude)
	if selected == nil || selected.ID != "node-3" {
		t.Errorf("SelectRandom with exclude returned %v, want node-3", selected)
	}
}

func TestPhiAccrualDetector(t *testing.T) {
	detector := NewPhiAccrualDetector()

	// Record several heartbeats
	for i := 0; i < 10; i++ {
		detector.RecordHeartbeat("node-1")
		time.Sleep(10 * time.Millisecond)
	}

	// Should not be suspect immediately
	if detector.IsSuspect("node-1") {
		t.Error("node-1 should not be suspect right after heartbeats")
	}

	// Unknown node should have phi = 0
	if detector.Phi("unknown") != 0.0 {
		t.Error("unknown node should have phi = 0")
	}
}

func TestPhiAccrualDetection(t *testing.T) {
	detector := NewPhiAccrualDetector()
	detector.phiThreshold = 0.5 // Very low threshold for fast test
	detector.minStdDev = 1 * time.Millisecond

	// Record enough heartbeats to build stats
	for i := 0; i < 10; i++ {
		detector.RecordHeartbeat("node-1")
		time.Sleep(5 * time.Millisecond)
	}

	// Wait much longer than the interval
	time.Sleep(500 * time.Millisecond)

	phi := detector.Phi("node-1")
	if phi <= 0 {
		t.Errorf("phi = %.2f, expected > 0 after silence", phi)
	}
	// With threshold 0.5 and long silence, should be suspect
	if !detector.IsSuspect("node-1") {
		t.Errorf("phi = %.2f, threshold = %.2f, should be suspect", phi, detector.phiThreshold)
	}
}

func TestRefute(t *testing.T) {
	ml := NewMemberList("node-1")
	ml.Add(&Member{ID: "node-1", Addr: "127.0.0.1", Port: 7946, State: Alive, Incarnation: 1})

	inc := ml.Refute()
	if inc != 2 {
		t.Errorf("Refute returned inc=%d, want 2", inc)
	}

	local := ml.Local()
	if local.State != Alive {
		t.Errorf("local state = %v, want Alive", local.State)
	}
}

func TestSWIMCreation(t *testing.T) {
	swim, err := NewSWIM(Config{
		NodeID:   "node-1",
		BindAddr: "127.0.0.1",
		BindPort: 0, // Random port
	})
	if err != nil {
		t.Fatal(err)
	}

	if swim.AliveCount() != 1 {
		t.Errorf("AliveCount = %d, want 1 (self)", swim.AliveCount())
	}

	swim.transport.Close()
}

func TestSWIMStartStop(t *testing.T) {
	swim, err := NewSWIM(Config{
		NodeID:        "node-1",
		BindAddr:      "127.0.0.1",
		BindPort:      0,
		ProbeInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := swim.Start(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	swim.Stop()
}

func TestSWIMTwoNodes(t *testing.T) {
	// Create two SWIM instances
	s1, err := NewSWIM(Config{
		NodeID:        "node-1",
		BindAddr:      "127.0.0.1",
		BindPort:      0,
		ProbeInterval: 100 * time.Millisecond,
		ProbeTimeout:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	s2, err := NewSWIM(Config{
		NodeID:        "node-2",
		BindAddr:      "127.0.0.1",
		BindPort:      0,
		ProbeInterval: 100 * time.Millisecond,
		ProbeTimeout:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Manually add each other
	addr1 := s1.LocalAddr()
	addr2 := s2.LocalAddr()

	// Extract ports from addresses
	s1.MemberList().Add(&Member{ID: "node-2", Addr: "127.0.0.1", Port: extractPort(addr2), State: Alive})
	s2.MemberList().Add(&Member{ID: "node-1", Addr: "127.0.0.1", Port: extractPort(addr1), State: Alive})

	s1.Start()
	defer s1.Stop()
	s2.Start()
	defer s2.Stop()

	// Wait for probe cycles
	time.Sleep(500 * time.Millisecond)

	// Both should see each other as alive
	if s1.AliveCount() < 2 {
		t.Errorf("s1 AliveCount = %d, want >= 2", s1.AliveCount())
	}
	if s2.AliveCount() < 2 {
		t.Errorf("s2 AliveCount = %d, want >= 2", s2.AliveCount())
	}
}

func TestMessageEncodeDecode(t *testing.T) {
	msg := &Message{
		Type:        MsgPing,
		SenderID:    "node-1",
		Incarnation: 5,
		Members: []MemberMsg{
			{ID: "node-2", Addr: "10.0.0.2", Port: 7946, State: Alive, Incarnation: 3},
		},
	}

	// Test that message can be marshaled/unmarshaled
	data, err := encodeMessage(msg)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := decodeMessage(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Type != MsgPing {
		t.Errorf("Type = %d, want %d", decoded.Type, MsgPing)
	}
	if decoded.SenderID != "node-1" {
		t.Errorf("SenderID = %s, want node-1", decoded.SenderID)
	}
	if len(decoded.Members) != 1 {
		t.Errorf("Members count = %d, want 1", len(decoded.Members))
	}
}
