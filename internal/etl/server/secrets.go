package server

import (
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// secretKeyPatterns are config key substrings that indicate a secret field.
var secretKeyPatterns = []string{
	"password", "passwd", "secret", "token", "api_key", "apikey", "credential", "private_key",
}

// isSecretKey reports whether a config key looks like a secret field.
func isSecretKey(key string) bool {
	lk := strings.ToLower(key)
	for _, pat := range secretKeyPatterns {
		if strings.Contains(lk, pat) {
			return true
		}
	}
	return false
}

// maskString redacts a secret for API responses.
// Short values become "****"; longer values keep first/last char (historical UI form).
func maskString(s string) string {
	if len(s) <= 2 {
		return "****"
	}
	return s[:1] + "****" + s[len(s)-1:]
}

// isSecretPlaceholder reports values that mean "keep the previously stored secret":
// empty string, fixed mask sentinels, or the historical maskString form.
func isSecretPlaceholder(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	if s == "" || s == "****" || s == "******" {
		return true
	}
	// maskString for len>2 always yields: <first>****<last> (exactly 6 runes/bytes for ASCII secrets).
	if len(s) == 6 && s[1:5] == "****" {
		return true
	}
	return false
}

// looksLikeMaskedSecret reports whether incoming equals a masked form of the real value.
func looksLikeMaskedSecret(incoming, real any) bool {
	rs, ok := real.(string)
	if !ok || rs == "" {
		return false
	}
	is, ok := incoming.(string)
	if !ok {
		return false
	}
	if is == "****" || is == "******" {
		return true
	}
	return is == maskString(rs)
}

// maskConfigSecrets recursively masks secret values in a config map for API responses.
func maskConfigSecrets(cfg map[string]any) map[string]any {
	if cfg == nil {
		return nil
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		if isSecretKey(k) {
			if s, ok := v.(string); ok && s != "" {
				out[k] = maskString(s)
			} else if v != nil && v != "" {
				out[k] = "****"
			} else {
				out[k] = v
			}
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			out[k] = maskConfigSecrets(vv)
		case []any:
			items := make([]any, len(vv))
			for i, item := range vv {
				if m, ok := item.(map[string]any); ok {
					items[i] = maskConfigSecrets(m)
				} else {
					items[i] = item
				}
			}
			out[k] = items
		default:
			out[k] = v
		}
	}
	return out
}

// maskSecretMap masks secrets for the connection catalog API.
// Connections always use a fixed sentinel ("******") so the UI can treat any
// non-empty secret field as "configured, value hidden".
func maskSecretMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if isSecretKey(k) {
			if s, ok := v.(string); ok && s != "" {
				out[k] = "******"
			} else {
				out[k] = v
			}
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			out[k] = maskSecretMap(vv)
		case []any:
			items := make([]any, len(vv))
			for i, item := range vv {
				if m, ok := item.(map[string]any); ok {
					items[i] = maskSecretMap(m)
				} else {
					items[i] = item
				}
			}
			out[k] = items
		default:
			out[k] = v
		}
	}
	return out
}

// preserveSecretConfig merges incoming config over existing, keeping previously stored
// secrets when the client resubmits a masked/empty placeholder from a GET response.
//
// This is required because GET /spec and GET /connections intentionally redact secrets
// for the UI; a full-form resubmit would otherwise persist the mask as the real password.
func preserveSecretConfig(incoming, existing map[string]any) map[string]any {
	if incoming == nil {
		return nil
	}
	if existing == nil {
		// Still drop pure placeholders so we don't store "******" as a password.
		return scrubSecretPlaceholders(incoming)
	}
	out := make(map[string]any, len(incoming))
	for k, v := range incoming {
		if isSecretKey(k) {
			old, hasOld := existing[k]
			if isSecretPlaceholder(v) || (hasOld && looksLikeMaskedSecret(v, old)) {
				if hasOld {
					out[k] = old
				}
				// If no previous secret, omit placeholder rather than storing the mask.
				continue
			}
			out[k] = v
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			oldMap, _ := existing[k].(map[string]any)
			out[k] = preserveSecretConfig(vv, oldMap)
		case []any:
			oldArr, _ := existing[k].([]any)
			items := make([]any, len(vv))
			for i, item := range vv {
				im, iok := item.(map[string]any)
				var om map[string]any
				if i < len(oldArr) {
					om, _ = oldArr[i].(map[string]any)
				}
				if iok {
					items[i] = preserveSecretConfig(im, om)
				} else {
					items[i] = item
				}
			}
			out[k] = items
		default:
			out[k] = v
		}
	}
	return out
}

// scrubSecretPlaceholders removes masked/empty secret values when there is no prior secret.
func scrubSecretPlaceholders(cfg map[string]any) map[string]any {
	if cfg == nil {
		return nil
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		if isSecretKey(k) && isSecretPlaceholder(v) {
			continue
		}
		switch vv := v.(type) {
		case map[string]any:
			out[k] = scrubSecretPlaceholders(vv)
		case []any:
			items := make([]any, len(vv))
			for i, item := range vv {
				if m, ok := item.(map[string]any); ok {
					items[i] = scrubSecretPlaceholders(m)
				} else {
					items[i] = item
				}
			}
			out[k] = items
		default:
			out[k] = v
		}
	}
	return out
}

func preserveLinearSpecSecrets(incoming, existing *pipeline.Spec) {
	if incoming == nil || existing == nil {
		return
	}
	incoming.Source.Config = preserveSecretConfig(incoming.Source.Config, existing.Source.Config)
	incoming.Sink.Config = preserveSecretConfig(incoming.Sink.Config, existing.Sink.Config)
	// Transforms: match by index when lengths align; otherwise only scrub placeholders.
	for i := range incoming.Transforms {
		if i < len(existing.Transforms) {
			incoming.Transforms[i].Config = preserveSecretConfig(incoming.Transforms[i].Config, existing.Transforms[i].Config)
		} else {
			incoming.Transforms[i].Config = scrubSecretPlaceholders(incoming.Transforms[i].Config)
		}
	}
	if incoming.DLQ != nil && existing.DLQ != nil {
		incoming.DLQ.Sink.Config = preserveSecretConfig(incoming.DLQ.Sink.Config, existing.DLQ.Sink.Config)
	}
}

func preserveDAGSpecSecrets(incoming, existing *orchestrator.PipelineSpec) {
	if incoming == nil || existing == nil {
		return
	}
	// Match nodes by ID so reordering does not drop secrets.
	oldByID := make(map[string]*orchestrator.Node, len(existing.DAG.Nodes))
	for _, n := range existing.DAG.Nodes {
		if n != nil && n.ID != "" {
			oldByID[n.ID] = n
		}
	}
	for i, n := range incoming.DAG.Nodes {
		if n == nil {
			continue
		}
		if old, ok := oldByID[n.ID]; ok {
			incoming.DAG.Nodes[i].Config = preserveSecretConfig(n.Config, old.Config)
		} else {
			incoming.DAG.Nodes[i].Config = scrubSecretPlaceholders(n.Config)
		}
	}
}

func maskSpecSecrets(spec *pipeline.Spec) *pipeline.Spec {
	if spec == nil {
		return nil
	}
	cp := *spec
	cp.Source.Config = maskConfigSecrets(cp.Source.Config)
	cp.Sink.Config = maskConfigSecrets(cp.Sink.Config)
	if cp.Transforms != nil {
		cp.Transforms = append([]pipeline.TransformSpec(nil), cp.Transforms...)
		for i := range cp.Transforms {
			cp.Transforms[i].Config = maskConfigSecrets(cp.Transforms[i].Config)
		}
	}
	if cp.DLQ != nil {
		dlq := *cp.DLQ
		dlq.Sink.Config = maskConfigSecrets(dlq.Sink.Config)
		cp.DLQ = &dlq
	}
	return &cp
}

func maskDAGSpecSecrets(spec *orchestrator.PipelineSpec) *orchestrator.PipelineSpec {
	if spec == nil {
		return nil
	}
	cp := *spec
	if cp.DAG.Nodes != nil {
		cp.DAG.Nodes = append([]*orchestrator.Node(nil), cp.DAG.Nodes...)
		for i, n := range cp.DAG.Nodes {
			if n == nil {
				continue
			}
			nc := *n
			nc.Config = maskConfigSecrets(n.Config)
			cp.DAG.Nodes[i] = &nc
		}
	}
	return &cp
}
