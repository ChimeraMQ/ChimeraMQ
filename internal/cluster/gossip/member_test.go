package gossip

import (
	"testing"
	"time"
)

func TestMemberStateString(t *testing.T) {
	tests := []struct {
		state MemberState
		want  string
	}{
		{Alive, "alive"},
		{Suspect, "suspect"},
		{Dead, "dead"},
		{MemberState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("MemberState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestMemberString(t *testing.T) {
	m := &Member{
		ID:          "node-1",
		Addr:        "10.0.0.1",
		Port:        7946,
		State:       Alive,
		Incarnation: 5,
	}
	s := m.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestMemberListRemove(t *testing.T) {
	ml := NewMemberList("local")
	ml.Add(&Member{ID: "node-1", State: Alive})
	ml.Remove("node-1")

	if ml.Get("node-1") != nil {
		t.Error("member should be removed")
	}
}

func TestMemberListAliveMembers(t *testing.T) {
	ml := NewMemberList("local")
	ml.Add(&Member{ID: "local", State: Alive})
	ml.Add(&Member{ID: "node-1", State: Alive})
	ml.Add(&Member{ID: "node-2", State: Dead})
	ml.Add(&Member{ID: "node-3", State: Suspect})

	alive := ml.AliveMembers()
	if len(alive) != 1 {
		t.Errorf("AliveMembers() = %d, want 1", len(alive))
	}
}

func TestMemberListSetState(t *testing.T) {
	ml := NewMemberList("local")
	ml.Add(&Member{ID: "node-1", State: Alive, Incarnation: 1})

	ml.SetState("node-1", Suspect, 2)
	m := ml.Get("node-1")
	if m.State != Suspect {
		t.Errorf("state = %v, want Suspect", m.State)
	}

	// Old incarnation should be ignored
	ml.SetState("node-1", Alive, 1)
	m = ml.Get("node-1")
	if m.State != Suspect {
		t.Error("old incarnation should not update state")
	}
}

func TestMemberListSize(t *testing.T) {
	ml := NewMemberList("local")
	if ml.Size() != 0 {
		t.Error("new list should have size 0")
	}

	ml.Add(&Member{ID: "node-1", State: Alive})
	ml.Add(&Member{ID: "node-2", State: Alive})

	if ml.Size() != 2 {
		t.Errorf("Size() = %d, want 2", ml.Size())
	}
}

func TestPhiAccrualDetectorRemove(t *testing.T) {
	d := NewPhiAccrualDetector()
	d.RecordHeartbeat("node-1")
	d.RecordHeartbeat("node-1")

	d.Remove("node-1")
	// After remove, Phi should return 0 (no window)
	if phi := d.Phi("node-1"); phi != 0 {
		t.Errorf("Phi after Remove = %v, want 0", phi)
	}
}

func TestPhiAccrualDetectorMultipleHeartbeats(t *testing.T) {
	d := NewPhiAccrualDetector()

	// Record several heartbeats with small intervals
	for i := 0; i < 5; i++ {
		d.RecordHeartbeat("node-1")
		time.Sleep(10 * time.Millisecond)
	}

	// Should not be suspect right after heartbeats
	if d.IsSuspect("node-1") {
		t.Error("should not suspect node with recent heartbeats")
	}
}

func TestMemberListLocal(t *testing.T) {
	ml := NewMemberList("local")
	ml.Add(&Member{ID: "local", Addr: "0.0.0.0", Port: 7946, State: Alive})

	local := ml.Local()
	if local == nil {
		t.Fatal("Local() should not be nil")
	}
	if local.ID != "local" {
		t.Errorf("Local().ID = %q, want local", local.ID)
	}
}

func TestMemberListAllExcludesLocal(t *testing.T) {
	ml := NewMemberList("local")
	ml.Add(&Member{ID: "local", State: Alive})
	ml.Add(&Member{ID: "node-1", State: Alive})
	ml.Add(&Member{ID: "node-2", State: Alive})

	all := ml.All()
	for _, m := range all {
		if m.ID == "local" {
			t.Error("All() should exclude local node")
		}
	}
}

func TestRandomIndex(t *testing.T) {
	if randomIndex(0) != 0 {
		t.Error("randomIndex(0) should be 0")
	}
	if randomIndex(-1) != 0 {
		t.Error("randomIndex(-1) should be 0")
	}
	// Should return a value in [0, n)
	for i := 0; i < 100; i++ {
		idx := randomIndex(10)
		if idx < 0 || idx >= 10 {
			t.Fatalf("randomIndex(10) = %d, out of range", idx)
		}
	}
}
