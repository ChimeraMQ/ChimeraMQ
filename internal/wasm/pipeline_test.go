package wasm

import (
	"context"
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func TestPipelinePassthru(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("passthru", testPassthruWasm(t))

	pipeline := NewPipeline([]TransformStage{{Module: "passthru"}}, PolicySkip)
	env := &message.Envelope{Payload: []byte("hello")}

	result, err := pipeline.Apply(context.Background(), rt, env)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("result should not be nil (passthru)")
	}
	if string(result.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", result.Payload, "hello")
	}
}

func TestPipelineEmptyStages(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	pipeline := NewPipeline(nil, PolicySkip)
	env := &message.Envelope{Payload: []byte("hello")}

	result, err := pipeline.Apply(context.Background(), rt, env)
	if err != nil {
		t.Fatal(err)
	}
	if result != env {
		t.Error("empty pipeline should return same envelope")
	}
}

func TestPipelineRejectOnError(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	pipeline := NewPipeline([]TransformStage{{Module: "nonexistent"}}, PolicyReject)
	env := &message.Envelope{Payload: []byte("hello")}

	_, err := pipeline.Apply(context.Background(), rt, env)
	if err == nil {
		t.Error("should reject on missing module")
	}
}

func TestPipelineSkipOnError(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	pipeline := NewPipeline([]TransformStage{{Module: "nonexistent"}}, PolicySkip)
	env := &message.Envelope{Payload: []byte("hello")}

	result, err := pipeline.Apply(context.Background(), rt, env)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("skip policy should return original on error")
	}
	if string(result.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", result.Payload, "hello")
	}
}

func TestPipelineMultiStage(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("s1", testPassthruWasm(t))
	rt.Compile("s2", testPassthruWasm(t))

	pipeline := NewPipeline([]TransformStage{{Module: "s1"}, {Module: "s2"}}, PolicySkip)
	env := &message.Envelope{Payload: []byte("hello")}

	result, err := pipeline.Apply(context.Background(), rt, env)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("should not be filtered")
	}
}
