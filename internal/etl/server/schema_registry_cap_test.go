package server

import (
	"context"
	"testing"
)

// CH-C3 follow-up: schema registry is a capability/preflight signal only.
func TestKafkaSourceSchemaRegistryCapability(t *testing.T) {
	descriptors := connectorDescriptors()
	k := findDescriptor(descriptors, "source", "kafka")
	if k == nil {
		t.Fatal("missing kafka source descriptor")
	}
	if !contains(k.Capabilities, "schema_registry") {
		t.Fatalf("kafka capabilities = %#v, want schema_registry", k.Capabilities)
	}
}

func TestKafkaSchemaRegistryGuidanceWarnsNotConsumed(t *testing.T) {
	spec := multiTablePreflightSpec("mysql", map[string]any{"database": "target"})
	spec.Source.Type = "kafka"
	spec.Source.Config = map[string]any{
		"brokers":              []string{"localhost:9092"},
		"topic":                "t",
		"schema_registry_url":  "http://registry:8081",
	}
	result := &PreflightResult{Passed: true}
	(&Server{}).checkSchemaCompatibility(context.Background(), spec, &schemaPreflightSink{}, result)

	found := false
	for _, g := range result.Guidance {
		if g.Code == "schema-registry-not-consumed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("guidance = %#v, want schema-registry-not-consumed", result.Guidance)
	}
}
