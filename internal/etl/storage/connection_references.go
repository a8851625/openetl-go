package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var ErrConnectionReferenceConflict = errors.New("connection reference conflict")

// ConnectionReference identifies an endpoint in the current persisted spec.
// Display names are never used as its identity; historical versions are excluded.
type ConnectionReference struct {
	ConnectionName string `json:"connection_name"`
	PipelineID     string `json:"pipeline_id"`
	PipelineName   string `json:"pipeline_name"`
	Endpoint       string `json:"endpoint"`
	NodeID         string `json:"node_id,omitempty"`
	Kind           string `json:"kind"`
	Type           string `json:"type,omitempty"`
}

type UnreadableReferenceSpec struct {
	PipelineID   string `json:"pipeline_id"`
	PipelineName string `json:"pipeline_name"`
	Code         string `json:"code"`
}

type ConnectionReferenceReport struct {
	ConnectionName string                    `json:"connection_name"`
	Known          bool                      `json:"known"`
	References     []ConnectionReference     `json:"references"`
	Unreadable     []UnreadableReferenceSpec `json:"unreadable_specs"`
}

type ConnectionReferenceConflict struct {
	Code    string
	Message string
	Report  ConnectionReferenceReport
}

func (e *ConnectionReferenceConflict) Error() string { return e.Message }
func (e *ConnectionReferenceConflict) Unwrap() error { return ErrConnectionReferenceConflict }

type ConnectionReferenceStore interface {
	ConnectionReferences(context.Context, string) (*ConnectionReferenceReport, error)
}

type referenceEndpoint struct {
	Type          string `yaml:"type"`
	Connection    string `yaml:"connection"`
	ConnectionRef string `yaml:"connection_ref"`
}

var referenceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ConnectionKindForNodeKind matches the runtime DAG connection resolver.
func ConnectionKindForNodeKind(kind string) (string, bool) {
	switch kind {
	case "source", "sink":
		return kind, true
	case "transform", "fanout", "router", "tap", "rate_limiter", "enricher", "lookup":
		return "transform", true
	default:
		return "", false
	}
}

// ExtractConnectionReferences decodes only endpoint identity, never config or
// secrets. The SQL guard uses it after locking, including for encrypted/legacy
// rows, so a process-local cache cannot hide a current spec reference.
func ExtractConnectionReferences(row *PipelineRow, cipher *SpecCipher) ([]ConnectionReference, error) {
	if row == nil || strings.TrimSpace(row.SpecYAML) == "" {
		return nil, nil
	}
	text := row.SpecYAML
	if strings.HasPrefix(text, specEnvelopePrefix) {
		if cipher == nil {
			return nil, ErrSpecEncryptionKeyUnavailable
		}
		var err error
		text, err = cipher.Decrypt(text)
		if err != nil {
			return nil, err
		}
	}
	decoder := yaml.NewDecoder(strings.NewReader(text))
	var doc yaml.Node
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("saved spec is unreadable")
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("saved spec must contain one YAML document")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("saved spec is not a mapping")
	}
	var spec struct {
		Source     referenceEndpoint   `yaml:"source"`
		Transforms []referenceEndpoint `yaml:"transforms"`
		Sink       referenceEndpoint   `yaml:"sink"`
		DLQ        *struct {
			Sink referenceEndpoint `yaml:"sink"`
		} `yaml:"dlq"`
		DAG *struct {
			Nodes []*struct {
				ID            string `yaml:"id"`
				Kind          string `yaml:"kind"`
				Plugin        string `yaml:"plugin"`
				Connection    string `yaml:"connection"`
				ConnectionRef string `yaml:"connection_ref"`
			} `yaml:"nodes"`
		} `yaml:"dag"`
	}
	if err := doc.Decode(&spec); err != nil {
		// YAML errors can quote configuration values. Do not expose those
		// values through the reference API or its conflict message.
		return nil, fmt.Errorf("saved spec endpoint identity is unreadable")
	}
	var refs []ConnectionReference
	add := func(endpoint, nodeID, kind string, ep referenceEndpoint) error {
		ref := strings.TrimSpace(ep.Connection)
		if ref == "" {
			ref = strings.TrimSpace(ep.ConnectionRef)
		}
		if ref == "" {
			return nil
		}
		if !referenceNamePattern.MatchString(ref) {
			return fmt.Errorf("saved spec has an invalid connection reference")
		}
		refs = append(refs, ConnectionReference{ConnectionName: ref,
			PipelineID: row.ID, PipelineName: row.Name, Endpoint: endpoint,
			NodeID: nodeID, Kind: kind, Type: ep.Type})
		return nil
	}
	isDAG := false
	for i := 0; i+1 < len(doc.Content[0].Content); i += 2 {
		if doc.Content[0].Content[i].Value == "dag" {
			isDAG = true
		}
	}
	if isDAG {
		if spec.DAG == nil {
			return nil, fmt.Errorf("saved DAG identity is unreadable")
		}
		for i, node := range spec.DAG.Nodes {
			if node == nil {
				return nil, fmt.Errorf("saved DAG has an unreadable node")
			}
			kind, ok := ConnectionKindForNodeKind(node.Kind)
			if !ok {
				return nil, fmt.Errorf("saved DAG has an unknown node kind")
			}
			if err := add(fmt.Sprintf("dag.nodes[%d]", i), node.ID, kind,
				referenceEndpoint{Type: node.Plugin, Connection: node.Connection, ConnectionRef: node.ConnectionRef}); err != nil {
				return nil, err
			}
		}
		return refs, nil
	}
	if err := add("source", "", "source", spec.Source); err != nil {
		return nil, err
	}
	for i, ep := range spec.Transforms {
		if err := add(fmt.Sprintf("transforms[%d]", i), "", "transform", ep); err != nil {
			return nil, err
		}
	}
	if err := add("sink", "", "sink", spec.Sink); err != nil {
		return nil, err
	}
	if spec.DLQ != nil {
		if err := add("dlq.sink", "", "sink", spec.DLQ.Sink); err != nil {
			return nil, err
		}
	}
	return refs, nil
}
