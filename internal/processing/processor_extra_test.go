package processing

import (
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func TestTopologyStateString(t *testing.T) {
	if TopologyStopped.String() != "stopped" {
		t.Errorf("TopologyStopped.String() = %q", TopologyStopped.String())
	}
	if TopologyRunning.String() != "running" {
		t.Errorf("TopologyRunning.String() = %q", TopologyRunning.String())
	}
	if TopologyState(99).String() != "stopped" {
		t.Errorf("unknown state should default to stopped")
	}
}

func TestSetBroker(t *testing.T) {
	p := NewProcessor(t.TempDir())
	if p.broker != nil {
		t.Error("broker should be nil initially")
	}

	p.SetBroker(&mockBroker{})
	if p.broker == nil {
		t.Error("broker should be set")
	}
}

func TestRegisterTransform(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.RegisterTransform("test-mod", func(env *message.Envelope) (*message.Envelope, error) {
		return env, nil
	})

	if len(p.transforms) != 1 {
		t.Error("transform should be registered")
	}
}

func TestApplyOperatorsPassthrough(t *testing.T) {
	p := NewProcessor(t.TempDir())
	env := &message.Envelope{Payload: []byte("test")}

	// No transforms registered — should passthrough
	result, err := p.applyOperators(env, []OperatorSpec{
		{Type: OpMap, Module: "unknown-mod"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Error("should passthrough when no transform registered")
	}
}

func TestApplyOperatorsWithTransform(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.RegisterTransform("upper", func(env *message.Envelope) (*message.Envelope, error) {
		env.Payload = []byte("TRANSFORMED")
		return env, nil
	})

	env := &message.Envelope{Payload: []byte("test")}
	result, err := p.applyOperators(env, []OperatorSpec{
		{Type: OpMap, Module: "upper"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Payload) != "TRANSFORMED" {
		t.Errorf("payload = %q", result.Payload)
	}
}

func TestApplyOperatorsFilterDrop(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.RegisterTransform("drop-all", func(env *message.Envelope) (*message.Envelope, error) {
		return nil, nil // drop
	})

	env := &message.Envelope{Payload: []byte("test")}
	result, err := p.applyOperators(env, []OperatorSpec{
		{Type: OpFilter, Module: "drop-all"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("filter should drop when transform returns nil")
	}
}

func TestApplyOperatorsError(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.RegisterTransform("bad", func(env *message.Envelope) (*message.Envelope, error) {
		return nil, errTest
	})

	env := &message.Envelope{Payload: []byte("test")}
	_, err := p.applyOperators(env, []OperatorSpec{
		{Type: OpMap, Module: "bad"},
	})
	if err == nil {
		t.Error("expected error from transform")
	}
}

func TestApplyOperatorsNilInput(t *testing.T) {
	p := NewProcessor(t.TempDir())
	result, err := p.applyOperators(nil, []OperatorSpec{
		{Type: OpMap, Module: "mod"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("nil input should return nil")
	}
}

var errTest = func() error {
	return errTestStruct{}
}()

type errTestStruct struct{}

func (errTestStruct) Error() string { return "test error" }

type mockBroker struct{}

func (m *mockBroker) FetchMessages(string, uint32, uint64, int) ([]*message.Envelope, error) {
	return nil, nil
}
func (m *mockBroker) PublishMessage(string, *message.Envelope) (uint64, uint32, error) {
	return 0, 0, nil
}
func (m *mockBroker) TopicPartitions(string) int { return 1 }
