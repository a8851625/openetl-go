package checkpoint

import (
	"encoding/json"
	"fmt"
)

const envelopeVersion = 1

// Envelope is the checkpoint payload format for stateful pipelines. It keeps
// the source offset, state snapshot versions, and sink commit metadata together
// so recovery can reason about the exact boundary that was persisted.
type Envelope struct {
	Version      int                    `json:"version"`
	Source       json.RawMessage        `json:"source"`
	State        map[string]string      `json:"state,omitempty"`
	SinkCommit   map[string]any         `json:"sink_commit,omitempty"`
	DeliveryMode string                 `json:"delivery_mode,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// BuildEnvelope marshals a checkpoint envelope while preserving the source
// payload as raw JSON for backward-compatible source-specific positions.
func BuildEnvelope(source json.RawMessage, state map[string]string, sinkCommit map[string]any) (json.RawMessage, error) {
	if len(source) == 0 {
		source = json.RawMessage(`{}`)
	}
	if !json.Valid(source) {
		return nil, fmt.Errorf("source position is not valid JSON")
	}
	env := Envelope{
		Version:      envelopeVersion,
		Source:       append(json.RawMessage(nil), source...),
		State:        cloneStringMap(state),
		SinkCommit:   cloneAnyMap(sinkCommit),
		DeliveryMode: "at_least_once",
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint envelope: %w", err)
	}
	return raw, nil
}

// ParseEnvelope returns a checkpoint envelope when the payload uses the v1
// envelope format. Legacy source positions return ok=false without error.
func ParseEnvelope(raw json.RawMessage) (*Envelope, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	var probe struct {
		Version int             `json:"version"`
		Source  json.RawMessage `json:"source"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, false, fmt.Errorf("unmarshal checkpoint envelope: %w", err)
	}
	if probe.Version == 0 && len(probe.Source) == 0 {
		return nil, false, nil
	}
	if probe.Version != envelopeVersion {
		return nil, false, fmt.Errorf("unsupported checkpoint envelope version %d", probe.Version)
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, false, fmt.Errorf("unmarshal checkpoint envelope: %w", err)
	}
	if len(env.Source) == 0 {
		env.Source = json.RawMessage(`{}`)
	}
	if env.State == nil {
		env.State = map[string]string{}
	}
	if env.SinkCommit == nil {
		env.SinkCommit = map[string]any{}
	}
	return &env, true, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
