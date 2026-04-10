package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracer wraps OpenTelemetry tracing for ChimeraMQ.
type Tracer struct {
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	enabled  bool
}

// Config holds tracing configuration.
type Config struct {
	Enabled    bool    `yaml:"enabled"`
	Endpoint   string  `yaml:"endpoint"`    // OTLP gRPC endpoint (default: localhost:4317)
	Service    string  `yaml:"service"`     // Service name (default: chimera)
	Insecure   bool    `yaml:"insecure"`    // Use insecure gRPC connection
	SampleRate float64 `yaml:"sample_rate"` // 0.0-1.0 (default: 1.0)
}

// NewTracer creates and initializes a new OpenTelemetry tracer.
func NewTracer(cfg Config) (*Tracer, error) {
	if !cfg.Enabled {
		return &Tracer{enabled: false}, nil
	}

	if cfg.Endpoint == "" {
		cfg.Endpoint = "localhost:4317"
	}
	if cfg.Service == "" {
		cfg.Service = "chimera"
	}
	if cfg.SampleRate == 0 {
		cfg.SampleRate = 1.0
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	sampleRate := cfg.SampleRate
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sampleRate)),
	)

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Tracer{
		provider: provider,
		tracer:   provider.Tracer(cfg.Service),
		enabled:  true,
	}, nil
}

// Enabled returns whether tracing is active.
func (t *Tracer) Enabled() bool {
	return t.enabled
}

// StartSpan creates a new span.
func (t *Tracer) StartSpan(ctx context.Context, name string, attrs ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.enabled {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name, attrs...)
}

// Shutdown flushes pending spans and shuts down the provider.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.provider == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return t.provider.Shutdown(shutdownCtx)
}
