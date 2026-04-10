package processing

import (
	"testing"
)

func TestCreateTopology(t *testing.T) {
	p := NewProcessor(t.TempDir())

	spec := TopologySpec{
		Name: "test-topology",
		Source: SourceSpec{
			Topic: "input",
		},
		Operators: []OperatorSpec{
			{Type: OpFilter, Module: "filter-mod"},
		},
		Sink:        SinkSpec{Topic: "output"},
		Parallelism: 2,
	}

	if err := p.CreateTopology(spec); err != nil {
		t.Fatal(err)
	}

	topo, ok := p.GetTopology("test-topology")
	if !ok {
		t.Fatal("topology should exist")
	}
	if topo.State != TopologyStopped {
		t.Error("new topology should be stopped")
	}
	if topo.Spec.Parallelism != 2 {
		t.Errorf("Parallelism = %d, want 2", topo.Spec.Parallelism)
	}
}

func TestCreateTopologyDuplicate(t *testing.T) {
	p := NewProcessor(t.TempDir())

	spec := TopologySpec{Name: "dup"}
	p.CreateTopology(spec)

	if err := p.CreateTopology(spec); err != ErrTopologyExists {
		t.Errorf("error = %v, want ErrTopologyExists", err)
	}
}

func TestCreateTopologyDefaultParallelism(t *testing.T) {
	p := NewProcessor(t.TempDir())

	spec := TopologySpec{Name: "auto", Parallelism: 0}
	if err := p.CreateTopology(spec); err != nil {
		t.Fatal(err)
	}

	topo, _ := p.GetTopology("auto")
	if topo.Spec.Parallelism != 1 {
		t.Errorf("Parallelism = %d, want 1 (default)", topo.Spec.Parallelism)
	}
}

func TestStartTopology(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})

	if err := p.StartTopology("t1"); err != nil {
		t.Fatal(err)
	}

	topo, _ := p.GetTopology("t1")
	if topo.State != TopologyRunning {
		t.Error("topology should be running")
	}
}

func TestStartTopologyIdempotent(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.StartTopology("t1")

	if err := p.StartTopology("t1"); err != nil {
		t.Error("starting already-running topology should be idempotent")
	}
}

func TestStartTopologyNotFound(t *testing.T) {
	p := NewProcessor(t.TempDir())

	if err := p.StartTopology("nonexistent"); err != ErrTopologyNotFound {
		t.Errorf("error = %v, want ErrTopologyNotFound", err)
	}
}

func TestStopTopology(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.StartTopology("t1")
	p.StopTopology("t1")

	topo, _ := p.GetTopology("t1")
	if topo.State != TopologyStopped {
		t.Error("topology should be stopped")
	}
}

func TestStopTopologyNotFound(t *testing.T) {
	p := NewProcessor(t.TempDir())

	if err := p.StopTopology("nonexistent"); err != ErrTopologyNotFound {
		t.Errorf("error = %v, want ErrTopologyNotFound", err)
	}
}

func TestDeleteTopology(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.DeleteTopology("t1")

	if _, ok := p.GetTopology("t1"); ok {
		t.Error("topology should be deleted")
	}
}

func TestDeleteRunningTopology(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.StartTopology("t1")

	if err := p.DeleteTopology("t1"); err != ErrTopologyRunning {
		t.Errorf("error = %v, want ErrTopologyRunning", err)
	}
}

func TestDeleteTopologyNotFound(t *testing.T) {
	p := NewProcessor(t.TempDir())

	if err := p.DeleteTopology("nonexistent"); err != ErrTopologyNotFound {
		t.Errorf("error = %v, want ErrTopologyNotFound", err)
	}
}

func TestListTopologies(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "a"})
	p.CreateTopology(TopologySpec{Name: "b"})
	p.CreateTopology(TopologySpec{Name: "c"})

	names := p.ListTopologies()
	if len(names) != 3 {
		t.Fatalf("ListTopologies returned %d items, want 3", len(names))
	}

	m := map[string]bool{}
	for _, n := range names {
		m[n] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !m[want] {
			t.Errorf("missing topology %q", want)
		}
	}
}

func TestAutoStart(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "auto", AutoStart: true})
	p.CreateTopology(TopologySpec{Name: "manual", AutoStart: false})

	p.Start()

	auto, _ := p.GetTopology("auto")
	if auto.State != TopologyRunning {
		t.Error("auto-start topology should be running")
	}

	manual, _ := p.GetTopology("manual")
	if manual.State != TopologyStopped {
		t.Error("non-auto topology should stay stopped")
	}
}

func TestStopAll(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.CreateTopology(TopologySpec{Name: "t2"})
	p.StartTopology("t1")
	p.StartTopology("t2")

	p.Stop()

	for _, name := range []string{"t1", "t2"} {
		topo, _ := p.GetTopology(name)
		if topo.State != TopologyStopped {
			t.Errorf("%s should be stopped", name)
		}
	}
}

func TestClose(t *testing.T) {
	p := NewProcessor(t.TempDir())

	p.CreateTopology(TopologySpec{Name: "t1"})
	p.StartTopology("t1")

	p.Close()

	topo, _ := p.GetTopology("t1")
	if topo.State != TopologyStopped {
		t.Error("Close should stop all topologies")
	}
}
