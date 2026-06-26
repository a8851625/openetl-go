package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func (s *Server) resolvePipelineConnections(ctx context.Context, spec *pipeline.Spec) error {
	if spec == nil {
		return nil
	}
	if err := s.resolveLinearEndpoint(ctx, "source", &spec.Source.Type, &spec.Source.Config, spec.Source.Connection, spec.Source.ConnectionRef); err != nil {
		return fmt.Errorf("source connection: %w", err)
	}
	for i := range spec.Transforms {
		tr := &spec.Transforms[i]
		if err := s.resolveLinearEndpoint(ctx, "transform", &tr.Type, &tr.Config, tr.Connection, tr.ConnectionRef); err != nil {
			return fmt.Errorf("transforms[%d] connection: %w", i, err)
		}
	}
	if err := s.resolveLinearEndpoint(ctx, "sink", &spec.Sink.Type, &spec.Sink.Config, spec.Sink.Connection, spec.Sink.ConnectionRef); err != nil {
		return fmt.Errorf("sink connection: %w", err)
	}
	if spec.DLQ != nil {
		if err := s.resolveLinearEndpoint(ctx, "sink", &spec.DLQ.Sink.Type, &spec.DLQ.Sink.Config, spec.DLQ.Sink.Connection, spec.DLQ.Sink.ConnectionRef); err != nil {
			return fmt.Errorf("dlq sink connection: %w", err)
		}
	}
	return nil
}

func (s *Server) resolveDAGConnections(ctx context.Context, spec *orchestrator.PipelineSpec) error {
	if spec == nil {
		return nil
	}
	for _, node := range spec.DAG.Nodes {
		if node == nil {
			continue
		}
		kind, ok := connectionKindForNode(node.Kind)
		if !ok {
			continue
		}
		cfg := map[string]any(node.Config)
		if err := s.resolveLinearEndpoint(ctx, kind, &node.Plugin, &cfg, node.Connection, node.ConnectionRef); err != nil {
			return fmt.Errorf("node %s connection: %w", node.ID, err)
		}
		node.Config = map[string]interface{}(cfg)
	}
	return nil
}

func connectionKindForNode(kind orchestrator.NodeKind) (string, bool) {
	switch kind {
	case orchestrator.KindSource:
		return "source", true
	case orchestrator.KindSink:
		return "sink", true
	case orchestrator.KindTransform, orchestrator.KindFanout, orchestrator.KindRouter, orchestrator.KindTap,
		orchestrator.KindRateLimiter, orchestrator.KindEnricher, orchestrator.KindLookup:
		return "transform", true
	default:
		return "", false
	}
}

func (s *Server) resolveLinearEndpoint(ctx context.Context, kind string, typ *string, cfg *map[string]any, connection, connectionRef string) error {
	ref := normalizedConnectionRef(connection, connectionRef)
	if ref == "" {
		if *cfg == nil {
			*cfg = map[string]any{}
		}
		return nil
	}
	if !connectionNamePattern.MatchString(ref) {
		return fmt.Errorf("invalid connection name %q", ref)
	}
	conn, err := s.store.GetConnection(ctx, ref)
	if err != nil {
		return fmt.Errorf("load %q: %w", ref, err)
	}
	if conn == nil {
		return fmt.Errorf("%q not found", ref)
	}
	if conn.Kind != kind {
		return fmt.Errorf("%q is a %s connection, cannot use as %s", ref, conn.Kind, kind)
	}
	if strings.TrimSpace(*typ) == "" {
		*typ = conn.Type
	} else if *typ != conn.Type {
		return fmt.Errorf("%q type %q does not match configured type %q", ref, conn.Type, *typ)
	}
	*cfg = mergeConnectionConfig(conn, *cfg)
	return nil
}

func normalizedConnectionRef(connection, connectionRef string) string {
	if strings.TrimSpace(connection) != "" {
		return strings.TrimSpace(connection)
	}
	return strings.TrimSpace(connectionRef)
}

func mergeConnectionConfig(conn *storage.ConnectionEntry, overrides map[string]any) map[string]any {
	merged := cloneConfigMap(conn.Config)
	for k, v := range overrides {
		merged[k] = v
	}
	return merged
}

func cloneConfigMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneConfigValue(v)
	}
	return out
}

func cloneConfigValue(v any) any {
	switch vv := v.(type) {
	case map[string]any:
		return cloneConfigMap(vv)
	case []any:
		items := make([]any, len(vv))
		for i, item := range vv {
			items[i] = cloneConfigValue(item)
		}
		return items
	default:
		return v
	}
}
