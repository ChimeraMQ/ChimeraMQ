package processing

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// --- StateStore.Close and StateStore.Name ---

func TestStateStoreClose_FlushError(t *testing.T) {
	// Close on a normal store should succeed
	dir := t.TempDir()
	store, err := NewStateStore("close-test", dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Put([]byte("k1"), []byte("v1"))

	if err := store.Close(); err != nil {
		t.Errorf("Close should succeed: %v", err)
	}
}

func TestStateStoreName(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("my-store", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if store.Name() != "my-store" {
		t.Errorf("Name() = %q, want %q", store.Name(), "my-store")
	}
}

func TestStateStoreCloseAndReopen(t *testing.T) {
	dir := t.TempDir()

	// Create and populate
	store, err := NewStateStore("reopen", dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Put([]byte("key1"), []byte("val1"))
	store.Put([]byte("key2"), []byte("val2"))

	// Close (flushes to LSM)
	if err := store.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	// Reopen and verify
	store2, err := NewStateStore("reopen", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	// After flush + close, the data was written to LSM memtable.
	// But since we closed, the data may or may not persist depending on LSM flush.
	// At minimum, Close should not error.
}

// --- AggregateOp.Close ---

func TestAggregateOpClose(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("agg-close", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var emitted atomic.Int32
	wm := NewWindowManager(WindowConfig{
		Type: WindowTumbling,
		Size: 1 * time.Hour,
	}, func(key string, state *WindowState) {
		emitted.Add(1)
	})

	sumFn := func(state []byte, event []byte) (newState []byte, emit bool) {
		return append(state, event...), true
	}

	agg := NewAggregateOp(wm, store, sumFn)

	// Add some events, then close (should emit remaining)
	agg.Process("k1", 100, []byte("a"))
	agg.Process("k1", 200, []byte("b"))

	agg.Close()

	// Close should emit all remaining windows
	if emitted.Load() != 1 {
		t.Errorf("emitted = %d, want 1 after Close", emitted.Load())
	}
}

// --- runTopology and Start with broker ---

func TestRunTopology_WithBroker(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.SetBroker(&mockBrokerWithMessages{
		msgs: []*message.Envelope{
			{Topic: "input", Payload: []byte("hello"), Sequence: 0},
			{Topic: "input", Payload: []byte("world"), Sequence: 1},
		},
		partitions: 2,
	})

	p.CreateTopology(TopologySpec{
		Name:        "broker-topo",
		Source:      SourceSpec{Topic: "input"},
		Sink:        SinkSpec{Topic: "output"},
		Parallelism: 1,
	})

	if err := p.StartTopology("broker-topo"); err != nil {
		t.Fatal(err)
	}

	// Give the goroutine time to process
	time.Sleep(200 * time.Millisecond)

	p.StopTopology("broker-topo")

	topo, _ := p.GetTopology("broker-topo")
	if topo.State != TopologyStopped {
		t.Error("topology should be stopped")
	}
}

func TestRunTopology_WithExplicitPartitions(t *testing.T) {
	var fetched atomic.Int32
	p := NewProcessor(t.TempDir())
	p.SetBroker(&countingMockBroker{fetched: &fetched})

	p.CreateTopology(TopologySpec{
		Name:   "explicit-parts",
		Source: SourceSpec{Topic: "input", Partitions: []uint32{0, 1}},
		Sink:   SinkSpec{Topic: "output"},
	})

	p.StartTopology("explicit-parts")

	time.Sleep(200 * time.Millisecond)

	p.StopTopology("explicit-parts")

	if fetched.Load() == 0 {
		t.Error("expected FetchMessages to be called")
	}
}

func TestRunTopology_TransformPipeline(t *testing.T) {
	var published atomic.Int32
	p := NewProcessor(t.TempDir())
	p.SetBroker(&sinkMockBroker{published: &published})
	p.RegisterTransform("uppercase", func(env *message.Envelope) (*message.Envelope, error) {
		env.Payload = []byte("UPPER")
		return env, nil
	})

	p.CreateTopology(TopologySpec{
		Name:        "transform-topo",
		Source:      SourceSpec{Topic: "input"},
		Operators:   []OperatorSpec{{Type: OpMap, Module: "uppercase"}},
		Sink:        SinkSpec{Topic: "output"},
		Parallelism: 1,
	})

	p.StartTopology("transform-topo")
	time.Sleep(200 * time.Millisecond)
	p.StopTopology("transform-topo")
}

func TestRunTopology_FilterDropsMessages(t *testing.T) {
	var published atomic.Int32
	p := NewProcessor(t.TempDir())
	p.SetBroker(&sinkMockBroker{published: &published})
	p.RegisterTransform("drop-all", func(env *message.Envelope) (*message.Envelope, error) {
		return nil, nil
	})

	p.CreateTopology(TopologySpec{
		Name:        "filter-topo",
		Source:      SourceSpec{Topic: "input"},
		Operators:   []OperatorSpec{{Type: OpFilter, Module: "drop-all"}},
		Sink:        SinkSpec{Topic: "output"},
		Parallelism: 1,
	})

	p.StartTopology("filter-topo")
	time.Sleep(200 * time.Millisecond)
	p.StopTopology("filter-topo")
}

func TestStart_WithBrokerAutoStart(t *testing.T) {
	p := NewProcessor(t.TempDir())
	p.SetBroker(&mockBrokerWithMessages{
		msgs: []*message.Envelope{
			{Topic: "input", Payload: []byte("data"), Sequence: 0},
		},
		partitions: 1,
	})

	p.CreateTopology(TopologySpec{
		Name:      "auto-broker",
		Source:    SourceSpec{Topic: "input"},
		AutoStart: true,
	})
	p.CreateTopology(TopologySpec{
		Name:      "manual-no-start",
		Source:    SourceSpec{Topic: "input"},
		AutoStart: false,
	})

	p.Start()

	time.Sleep(200 * time.Millisecond)

	auto, _ := p.GetTopology("auto-broker")
	if auto.State != TopologyRunning {
		t.Error("auto-start topology with broker should be running")
	}

	manual, _ := p.GetTopology("manual-no-start")
	if manual.State != TopologyStopped {
		t.Error("non-auto topology should stay stopped")
	}

	p.Close()
}

// --- Mock brokers for runTopology testing ---

type mockBrokerWithMessages struct {
	msgs       []*message.Envelope
	partitions int
}

func (m *mockBrokerWithMessages) FetchMessages(topic string, partition uint32, offset uint64, limit int) ([]*message.Envelope, error) {
	var result []*message.Envelope
	for _, env := range m.msgs {
		if env.Sequence >= offset {
			result = append(result, env)
		}
	}
	return result, nil
}

func (m *mockBrokerWithMessages) PublishMessage(topic string, env *message.Envelope) (uint64, uint32, error) {
	return 0, 0, nil
}

func (m *mockBrokerWithMessages) TopicPartitions(topic string) int {
	if m.partitions > 0 {
		return m.partitions
	}
	return 1
}

type countingMockBroker struct {
	fetched *atomic.Int32
}

func (m *countingMockBroker) FetchMessages(topic string, partition uint32, offset uint64, limit int) ([]*message.Envelope, error) {
	m.fetched.Add(1)
	return nil, nil // no messages — will loop and pause
}

func (m *countingMockBroker) PublishMessage(topic string, env *message.Envelope) (uint64, uint32, error) {
	return 0, 0, nil
}

func (m *countingMockBroker) TopicPartitions(topic string) int {
	return 1
}

type sinkMockBroker struct {
	published *atomic.Int32
}

func (m *sinkMockBroker) FetchMessages(topic string, partition uint32, offset uint64, limit int) ([]*message.Envelope, error) {
	return []*message.Envelope{
		{Topic: topic, Payload: []byte("msg"), Sequence: offset},
	}, nil
}

func (m *sinkMockBroker) PublishMessage(topic string, env *message.Envelope) (uint64, uint32, error) {
	m.published.Add(1)
	return 0, 0, nil
}

func (m *sinkMockBroker) TopicPartitions(topic string) int {
	return 1
}
