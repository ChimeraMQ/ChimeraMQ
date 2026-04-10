package wasm

import (
	"context"
	"fmt"

	"github.com/chimeramq/chimera/internal/message"
)

// ErrorPolicy controls how transform errors are handled.
type ErrorPolicy uint8

const (
	PolicySkip   ErrorPolicy = 0 // Skip failed transform, pass original
	PolicyDLQ    ErrorPolicy = 1 // Route to DLQ
	PolicyReject ErrorPolicy = 2 // Reject the publish
)

// TransformStage is one stage in a transform pipeline.
type TransformStage struct {
	Module string // WASM module name
}

// TransformPipeline applies a sequence of WASM transforms to a message.
type TransformPipeline struct {
	stages []TransformStage
	policy ErrorPolicy
}

// NewPipeline creates a new transform pipeline.
func NewPipeline(stages []TransformStage, policy ErrorPolicy) *TransformPipeline {
	return &TransformPipeline{stages: stages, policy: policy}
}

// Stages returns the pipeline stages.
func (p *TransformPipeline) Stages() []TransformStage { return p.stages }

// Apply runs all transform stages on the envelope.
// Returns nil if the message was filtered (dropped).
// Returns the (possibly modified) envelope if all stages passed.
func (p *TransformPipeline) Apply(ctx context.Context, rt *Runtime, env *message.Envelope) (*message.Envelope, error) {
	if len(p.stages) == 0 {
		return env, nil
	}

	payload := env.Payload

	for _, stage := range p.stages {
		result, err := rt.Transform(ctx, stage.Module, payload)
		if err != nil {
			switch p.policy {
			case PolicySkip:
				continue // use current payload, move to next stage
			case PolicyReject:
				return nil, fmt.Errorf("transform stage %q failed: %w", stage.Module, err)
			case PolicyDLQ:
				return nil, fmt.Errorf("transform stage %q failed (DLQ): %w", stage.Module, err)
			}
		}

		if result.Drop {
			return nil, nil // message filtered
		}

		if !result.Passthru && result.Data != nil {
			payload = result.Data
		}
	}

	// If payload was modified, create a new envelope
	if string(payload) != string(env.Payload) {
		modified := *env // shallow copy
		modified.Payload = payload
		return &modified, nil
	}

	return env, nil
}
