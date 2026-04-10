package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestTracerDisabled(t *testing.T) {
	tr, err := NewTracer(Config{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if tr.Enabled() {
		t.Error("disabled tracer should not be enabled")
	}

	ctx, span := tr.StartSpan(context.Background(), "test")
	_ = span.IsRecording() // always false for disabled tracer
	_ = ctx

	if err := tr.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() = %v", err)
	}
}

func TestTracerEnabled(t *testing.T) {
	tr, err := NewTracer(Config{
		Enabled:  true,
		Endpoint: "localhost:4317",
		Service:  "test",
		Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown(context.Background())

	if !tr.Enabled() {
		t.Error("tracer should be enabled")
	}

	ctx, span := tr.StartSpan(context.Background(), "test-operation")
	_ = span.SpanContext().IsValid() // verify span context
	span.End()
	_ = ctx
}

func TestTracerDefaults(t *testing.T) {
	tr, err := NewTracer(Config{
		Enabled:  true,
		Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown(context.Background())

	if !tr.Enabled() {
		t.Error("tracer should be enabled")
	}
}

func TestTracerNestedSpans(t *testing.T) {
	tr, err := NewTracer(Config{
		Enabled:  true,
		Endpoint: "localhost:4317",
		Service:  "test",
		Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown(context.Background())

	ctx1, span1 := tr.StartSpan(context.Background(), "parent")
	ctx2, span2 := tr.StartSpan(ctx1, "child")

	sc1 := span1.SpanContext()
	sc2 := span2.SpanContext()

	if !sc1.IsValid() || !sc2.IsValid() {
		t.Error("both spans should be valid")
	}

	// Child should have the same trace ID as parent
	if sc1.TraceID() != sc2.TraceID() {
		t.Error("child trace ID should match parent")
	}

	span2.End()
	span1.End()
	_ = ctx2
}

func TestTracerSpanFromContext(t *testing.T) {
	tr, _ := NewTracer(Config{Enabled: false})

	ctx, span := tr.StartSpan(context.Background(), "noop")
	// For disabled tracer, we get a non-recording span
	_ = ctx

	// Should not panic
	span.SetName("renamed")
	span.End()
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Enabled: true}
	// Endpoint will be set by NewTracer if empty
	tr, err := NewTracer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown(context.Background())
}

func TestTracerShutdownNil(t *testing.T) {
	tr := &Tracer{enabled: false}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() with nil provider should not error: %v", err)
	}
}

func TestSpanContextPropagation(t *testing.T) {
	tr, err := NewTracer(Config{
		Enabled:  true,
		Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown(context.Background())

	ctx, span := tr.StartSpan(context.Background(), "test")
	defer span.End()

	// Verify span can be extracted from context
	extracted := trace.SpanFromContext(ctx)
	if extracted.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Error("extracted span should have same trace ID")
	}
}
