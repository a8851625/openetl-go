package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IBM/sarama"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestConnectionCatalogCRUDAndHealth(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	body, err := json.Marshal(map[string]any{
		"name": "identity-dev",
		"kind": "transform",
		"type": "identity",
		"config": map[string]any{
			"api_token": "secret-value",
			"nested": map[string]any{
				"password": "pw",
			},
		},
		"test": true,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/connections", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST connection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200", resp.StatusCode)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	conn := created["connection"].(map[string]any)
	cfg := conn["config"].(map[string]any)
	if cfg["api_token"] != "******" {
		t.Fatalf("secret not masked: %#v", cfg)
	}
	nested := cfg["nested"].(map[string]any)
	if nested["password"] != "******" {
		t.Fatalf("nested secret not masked: %#v", nested)
	}

	testResp, err := http.Post(ts.URL+"/api/v2/connections/identity-dev/test", "application/json", bytes.NewReader([]byte(`{"open":false}`)))
	if err != nil {
		t.Fatalf("POST saved test: %v", err)
	}
	defer testResp.Body.Close()
	if testResp.StatusCode != http.StatusOK {
		t.Fatalf("test status = %d, want 200", testResp.StatusCode)
	}
	var tested map[string]any
	if err := json.NewDecoder(testResp.Body).Decode(&tested); err != nil {
		t.Fatalf("decode test: %v", err)
	}
	if tested["ok"] != true {
		t.Fatalf("test result = %#v", tested)
	}

	listResp, err := http.Get(ts.URL + "/api/v2/connections")
	if err != nil {
		t.Fatalf("GET connections: %v", err)
	}
	defer listResp.Body.Close()
	var listed struct {
		Connections []map[string]any `json:"connections"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Connections) != 1 || listed.Connections[0]["last_status"] != "ok" {
		t.Fatalf("unexpected list: %#v", listed.Connections)
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/connections/identity-dev", nil)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE connection: %v", err)
	}
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", delResp.StatusCode)
	}
}

func TestPipelineUsesSavedConnections(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "demo-source",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"interval_ms": 0,
			"count":       1,
			"fields": []map[string]any{
				{"name": "id", "type": "counter"},
			},
		},
	})
	outDir := t.TempDir()
	saveTestConnection(t, ts.URL, map[string]any{
		"name":   "file-target",
		"kind":   "sink",
		"type":   "file_sink",
		"config": map[string]any{"output_dir": outDir, "format": "json"},
	})

	spec := pipeline.Spec{
		Name:   "connection-ref-linear",
		Source: pipeline.SourceSpec{Connection: "demo-source", Config: map[string]any{"count": 2}},
		Sink:   pipeline.SinkSpec{ConnectionRef: "file-target"},
	}
	body := mustPipelineJSON(t, spec)
	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST pipeline: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		t.Fatalf("create status = %d, body=%#v", resp.StatusCode, got)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("create response missing id: %#v", created)
	}

	s.mu.RLock()
	resolved := s.specs[id]
	s.mu.RUnlock()
	if resolved == nil {
		t.Fatal("pipeline was not created")
	}
	if resolved.Source.Type != "demo" || resolved.Sink.Type != "file_sink" {
		t.Fatalf("connection types not resolved: source=%q sink=%q", resolved.Source.Type, resolved.Sink.Type)
	}
	if resolved.Source.Config["interval_ms"] != float64(0) || resolved.Source.Config["count"] != float64(2) {
		t.Fatalf("source config not merged with override: %#v", resolved.Source.Config)
	}
	if resolved.Sink.Config["output_dir"] != outDir || resolved.Sink.Config["format"] != "json" {
		t.Fatalf("sink config not merged: %#v", resolved.Sink.Config)
	}
}

func TestDAGValidateResolvesSavedConnections(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "dag-demo-source",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"interval_ms": 0,
			"count":       1,
		},
	})
	saveTestConnection(t, ts.URL, map[string]any{
		"name":   "dag-file-target",
		"kind":   "sink",
		"type":   "file_sink",
		"config": map[string]any{"output_dir": t.TempDir()},
	})

	spec := map[string]any{
		"name": "connection-ref-dag",
		"dag": map[string]any{
			"nodes": []map[string]any{
				{"id": "src", "kind": "source", "connection": "dag-demo-source", "config": map[string]any{"count": 3}},
				{"id": "snk", "kind": "sink", "connection_ref": "dag-file-target"},
			},
			"edges": []map[string]any{{"from": "src", "to": "snk"}},
		},
	}
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal validate request: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("validate status = %d", resp.StatusCode)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode validate: %v", err)
	}
	if got["valid"] != true {
		t.Fatalf("validate response = %#v", got)
	}
	resolved := got["spec"].(map[string]any)
	dag := resolved["dag"].(map[string]any)
	nodes := dag["nodes"].([]any)
	src := nodes[0].(map[string]any)
	if src["plugin"] != "demo" {
		t.Fatalf("source plugin not resolved: %#v", src)
	}
	srcCfg := src["config"].(map[string]any)
	if srcCfg["interval_ms"] != float64(0) || srcCfg["count"] != float64(3) {
		t.Fatalf("source config not merged with override: %#v", srcCfg)
	}
	snk := nodes[1].(map[string]any)
	if snk["plugin"] != "file_sink" {
		t.Fatalf("sink plugin not resolved: %#v", snk)
	}
}

func TestConnectionContextIncludesRecommendationsAndDescriptor(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "demo-context",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"fields": []map[string]any{
				{"name": "id", "type": "counter"},
				{"name": "name", "type": "string"},
			},
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/demo-context/context")
	if err != nil {
		t.Fatalf("GET connection context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Descriptor *struct {
			Kind string `json:"kind"`
			Type string `json:"type"`
		} `json:"descriptor"`
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK     bool             `json:"ok"`
			Schema []map[string]any `json:"schema"`
			Sample []map[string]any `json:"sample"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode context: %v", err)
	}
	if got.Descriptor == nil || got.Descriptor.Kind != "source" || got.Descriptor.Type != "demo" {
		t.Fatalf("descriptor not returned: %#v", got.Descriptor)
	}
	if !got.Introspection.OK || len(got.Introspection.Schema) != 2 || len(got.Introspection.Sample) != 1 {
		t.Fatalf("unexpected demo introspection: %#v", got.Introspection)
	}
	if len(got.Recommendations) == 0 {
		t.Fatalf("expected recommendations, got none")
	}
}

func TestConnectionContextIntrospectsFileSource(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,Alice\n2,Bob\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	saveTestConnection(t, ts.URL, map[string]any{
		"name": "file-context",
		"kind": "source",
		"type": "file",
		"config": map[string]any{
			"path":       path,
			"format":     "csv",
			"has_header": true,
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/file-context/context")
	if err != nil {
		t.Fatalf("GET file context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Introspection struct {
			OK     bool `json:"ok"`
			Schema []struct {
				Name string `json:"name"`
			} `json:"schema"`
			Sample []struct {
				Data map[string]any `json:"data"`
			} `json:"sample"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode file context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("file introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Schema) != 2 || got.Introspection.Schema[0].Name != "id" || got.Introspection.Schema[1].Name != "name" {
		t.Fatalf("unexpected schema: %#v", got.Introspection.Schema)
	}
	if len(got.Introspection.Sample) != 2 || got.Introspection.Sample[0].Data["name"] != "Alice" {
		t.Fatalf("unexpected sample: %#v", got.Introspection.Sample)
	}
}

func TestConnectionContextIntrospectsFileSinkTarget(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	outDir := filepath.Join(t.TempDir(), "orders")
	saveTestConnection(t, ts.URL, map[string]any{
		"name": "file-sink-context",
		"kind": "sink",
		"type": "file_sink",
		"config": map[string]any{
			"output_dir": outDir,
			"format":     "json",
			"prefix":     "orders_",
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/file-sink-context/context")
	if err != nil {
		t.Fatalf("GET file sink context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK      bool `json:"ok"`
			Targets []struct {
				Kind     string `json:"kind"`
				Location string `json:"location"`
				Prefix   string `json:"prefix"`
				Format   string `json:"format"`
				Exists   bool   `json:"exists"`
				Writable bool   `json:"writable"`
			} `json:"targets"`
			Warnings []string `json:"warnings"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode file sink context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("file sink introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Targets) != 1 {
		t.Fatalf("targets = %#v, want one file target", got.Introspection.Targets)
	}
	target := got.Introspection.Targets[0]
	if target.Kind != "file" || target.Location != outDir || target.Prefix != "orders_" || target.Format != "json" || !target.Writable {
		t.Fatalf("unexpected target: %#v", target)
	}
	if len(got.Introspection.Warnings) != 0 {
		t.Fatalf("warnings = %#v, want none when prefix is set", got.Introspection.Warnings)
	}
	if !recommendationsContain(got.Recommendations, "sink.idempotency") {
		t.Fatalf("recommendations = %#v, want file sink idempotency guidance", got.Recommendations)
	}
}

func TestConnectionContextIntrospectsS3LocalFallback(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	outDir := t.TempDir()
	saveTestConnection(t, ts.URL, map[string]any{
		"name": "s3-fallback-context",
		"kind": "sink",
		"type": "s3",
		"config": map[string]any{
			"output_dir": outDir,
			"format":     "json",
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/s3-fallback-context/context")
	if err != nil {
		t.Fatalf("GET s3 context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Introspection struct {
			OK      bool `json:"ok"`
			Targets []struct {
				Kind     string `json:"kind"`
				Location string `json:"location"`
				Writable bool   `json:"writable"`
			} `json:"targets"`
			Warnings []string `json:"warnings"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode s3 context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("s3 fallback introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Targets) != 1 || got.Introspection.Targets[0].Kind != "file" || got.Introspection.Targets[0].Location != outDir || !got.Introspection.Targets[0].Writable {
		t.Fatalf("targets = %#v, want writable local fallback", got.Introspection.Targets)
	}
	if !warningsContainAny(got.Introspection.Warnings, "local file fallback") {
		t.Fatalf("warnings = %#v, want local fallback warning", got.Introspection.Warnings)
	}
}

func TestConnectionContextIntrospectsElasticsearchSink(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_cluster/health":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"green"}`))
		case "/customers/_mapping":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"customers":{"mappings":{"properties":{"id":{"type":"long"},"name":{"type":"keyword"},"amount":{"type":"double"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	defer es.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "es-context",
		"kind": "sink",
		"type": "elasticsearch",
		"config": map[string]any{
			"host":  es.URL,
			"index": "customers",
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/es-context/context")
	if err != nil {
		t.Fatalf("GET es context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK     bool `json:"ok"`
			Schema []struct {
				Name     string `json:"name"`
				DataType string `json:"data_type"`
			} `json:"schema"`
			Tables []struct {
				Name    string `json:"name"`
				Columns []struct {
					Name     string `json:"name"`
					DataType string `json:"data_type"`
				} `json:"columns"`
			} `json:"tables"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode es context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("es introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Schema) != 3 || got.Introspection.Schema[0].Name != "amount" || got.Introspection.Schema[0].DataType != "double" {
		t.Fatalf("unexpected es schema: %#v", got.Introspection.Schema)
	}
	if len(got.Introspection.Tables) != 1 || got.Introspection.Tables[0].Name != "customers" || len(got.Introspection.Tables[0].Columns) != 3 {
		t.Fatalf("unexpected es tables: %#v", got.Introspection.Tables)
	}
	if len(got.Recommendations) == 0 {
		t.Fatalf("expected sink recommendations, got none")
	}
}

func TestConnectionContextIntrospectsClickHouseHTTPSink(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(query, "SELECT 1"):
			_, _ = w.Write([]byte(`{"data":[{"ok":1}]}`))
		case strings.Contains(query, "system.databases"):
			_, _ = w.Write([]byte(`{"data":[{"name":"default"},{"name":"warehouse"}]}`))
		case strings.Contains(query, "system.tables"):
			_, _ = w.Write([]byte(`{"data":[{"name":"orders"},{"name":"users"}]}`))
		case strings.Contains(query, "system.columns"):
			_, _ = w.Write([]byte(`{"data":[{"name":"id","type":"Int64","is_in_primary_key":1},{"name":"name","type":"String","is_in_primary_key":0},{"name":"amount","type":"Float64","is_in_primary_key":0}]}`))
		case strings.Contains(query, "SELECT *"):
			_, _ = w.Write([]byte(`{"data":[{"id":1,"name":"Alice","amount":12.5}]}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unexpected query"}`))
		}
	}))
	defer ch.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "clickhouse-context",
		"kind": "sink",
		"type": "clickhouse",
		"config": map[string]any{
			"host":     ch.URL,
			"protocol": "http",
			"database": "warehouse",
			"table":    "orders",
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/clickhouse-context/context")
	if err != nil {
		t.Fatalf("GET clickhouse context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK        bool     `json:"ok"`
			Databases []string `json:"databases"`
			Schema    []struct {
				Name     string `json:"name"`
				DataType string `json:"data_type"`
			} `json:"schema"`
			Tables []struct {
				Database   string   `json:"database"`
				Name       string   `json:"name"`
				PrimaryKey []string `json:"primary_key"`
				Columns    []struct {
					Name     string `json:"name"`
					DataType string `json:"data_type"`
				} `json:"columns"`
			} `json:"tables"`
			Sample []struct {
				Data map[string]any `json:"data"`
			} `json:"sample"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode clickhouse context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("clickhouse introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Databases) != 2 || got.Introspection.Databases[1] != "warehouse" {
		t.Fatalf("unexpected databases: %#v", got.Introspection.Databases)
	}
	if len(got.Introspection.Schema) != 3 || got.Introspection.Schema[0].Name != "id" || got.Introspection.Schema[0].DataType != "Int64" {
		t.Fatalf("unexpected schema: %#v", got.Introspection.Schema)
	}
	if len(got.Introspection.Tables) != 2 || got.Introspection.Tables[0].Name != "orders" || len(got.Introspection.Tables[0].Columns) != 3 || len(got.Introspection.Tables[0].PrimaryKey) != 1 {
		t.Fatalf("unexpected tables: %#v", got.Introspection.Tables)
	}
	if len(got.Introspection.Sample) != 1 || got.Introspection.Sample[0].Data["name"] != "Alice" {
		t.Fatalf("unexpected sample: %#v", got.Introspection.Sample)
	}
	if len(got.Recommendations) == 0 {
		t.Fatalf("expected clickhouse sink recommendations, got none")
	}
}

func TestConnectionContextIntrospectsKafkaSink(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("ods.orders", 0, broker.BrokerID()).
			SetLeader("ods.orders", 1, broker.BrokerID()).
			SetLeader("other.topic", 0, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "kafka-sink-context",
		"kind": "sink",
		"type": "kafka",
		"config": map[string]any{
			"brokers": []string{broker.Addr()},
			"topic":   "ods.orders",
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/kafka-sink-context/context")
	if err != nil {
		t.Fatalf("GET kafka sink context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK     bool   `json:"ok"`
			Error  string `json:"error"`
			Topics []struct {
				Name       string `json:"name"`
				Partitions []struct {
					ID     int32 `json:"id"`
					Leader int32 `json:"leader"`
				} `json:"partitions"`
			} `json:"topics"`
			Warnings []string `json:"warnings"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode kafka sink context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("kafka sink introspection failed: %#v", got.Introspection)
	}
	var targetTopic *struct {
		Name       string `json:"name"`
		Partitions []struct {
			ID     int32 `json:"id"`
			Leader int32 `json:"leader"`
		} `json:"partitions"`
	}
	for i := range got.Introspection.Topics {
		if got.Introspection.Topics[i].Name == "ods.orders" {
			targetTopic = &got.Introspection.Topics[i]
			break
		}
	}
	if targetTopic == nil || len(targetTopic.Partitions) != 2 {
		t.Fatalf("topics = %#v, want ods.orders with two partitions", got.Introspection.Topics)
	}
	if len(got.Introspection.Warnings) != 0 {
		t.Fatalf("warnings = %#v, want none for existing topic", got.Introspection.Warnings)
	}
	if !recommendationsContain(got.Recommendations, "sink.config.key_column") || !recommendationsContain(got.Recommendations, "sink.config.auto_create_topic") {
		t.Fatalf("recommendations = %#v, want kafka sink recommendations", got.Recommendations)
	}
}

func TestKafkaTargetTopicWarning(t *testing.T) {
	result := &connectionIntrospection{
		Topics: []topicMetadata{{Name: "orders", Partitions: []partitionMetadata{{ID: 0}}}},
	}
	addKafkaTargetTopicWarning("missing-topic", result)
	if len(result.Warnings) != 1 || !strings.Contains(result.Warnings[0], "missing-topic") {
		t.Fatalf("warnings = %#v, want missing-topic warning", result.Warnings)
	}
	if got := result.Topics[len(result.Topics)-1].Name; got != "missing-topic" {
		t.Fatalf("topics = %#v, want placeholder missing-topic", result.Topics)
	}
}

func recommendationsContain(recs []map[string]any, field string) bool {
	for _, rec := range recs {
		if rec["field"] == field {
			return true
		}
	}
	return false
}

func warningsContainAny(warnings []string, needle string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, needle) {
			return true
		}
	}
	return false
}

func saveTestConnection(t *testing.T, baseURL string, body map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal connection: %v", err)
	}
	resp, err := http.Post(baseURL+"/api/v2/connections", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST connection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		t.Fatalf("save connection status = %d, body=%#v", resp.StatusCode, got)
	}
}

// TestConnectionSavePreservesMaskedSecrets ensures updating a connection with
// GET-masked secret placeholders does not overwrite the stored password.
func TestConnectionSavePreservesMaskedSecrets(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "mysql-src",
		"kind": "source",
		"type": "mysql_batch",
		"config": map[string]any{
			"host":     "db.example",
			"user":     "sync",
			"password": "real-password-xyz",
			"database": "app",
		},
	})

	// Resubmit with the catalog mask sentinel and a non-secret field change.
	saveTestConnection(t, ts.URL, map[string]any{
		"name": "mysql-src",
		"kind": "source",
		"type": "mysql_batch",
		"config": map[string]any{
			"host":     "db.example",
			"user":     "sync2",
			"password": "******",
			"database": "app",
		},
	})

	got, err := s.store.GetConnection(t.Context(), "mysql-src")
	if err != nil {
		t.Fatalf("GetConnection: %v", err)
	}
	if got == nil {
		t.Fatal("connection missing")
	}
	if got.Config["password"] != "real-password-xyz" {
		t.Fatalf("password overwritten: %#v", got.Config["password"])
	}
	if got.Config["user"] != "sync2" {
		t.Fatalf("user not updated: %#v", got.Config["user"])
	}
}

// TestPipelineUpdatePreservesMaskedSecrets ensures PUT /pipelines keeps real
// secrets when the editor resubmits a GET /spec payload with masked passwords.
func TestPipelineUpdatePreservesMaskedSecrets(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	outDir := t.TempDir()
	spec := pipeline.Spec{
		Name: "secret-pipe",
		Source: pipeline.SourceSpec{
			Type: "file",
			Config: map[string]any{
				"path":     filepath.Join(outDir, "in.jsonl"),
				"format":   "json",
				"password": "source-secret-value",
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "file_sink",
			Config: map[string]any{
				"output_dir": outDir,
				"format":     "json",
				"password":   "sink-secret-value",
			},
		},
	}
	// Seed an input file so file source open/preflight is happier if exercised.
	if err := os.WriteFile(filepath.Join(outDir, "in.jsonl"), []byte(`{"id":1}`+"\n"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	createResp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("POST pipeline: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(createResp.Body).Decode(&got)
		t.Fatalf("create status=%d body=%#v", createResp.StatusCode, got)
	}
	var created map[string]any
	if err := json.NewDecoder(createResp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("missing id: %#v", created)
	}

	// GET /spec returns masked secrets.
	getResp, err := http.Get(ts.URL + "/api/v2/pipelines/" + id + "/spec")
	if err != nil {
		t.Fatalf("GET spec: %v", err)
	}
	defer getResp.Body.Close()
	var getBody struct {
		Spec pipeline.Spec `json:"spec"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&getBody); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if getBody.Spec.Source.Config["password"] == "source-secret-value" {
		t.Fatalf("GET /spec did not mask source password: %#v", getBody.Spec.Source.Config["password"])
	}

	// Change a non-secret field and PUT the masked payload back.
	getBody.Spec.Sink.Config["format"] = "jsonl"
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines", bytes.NewReader(mustPipelineUpdateJSONWithID(t, id, getBody.Spec)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT pipeline: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(putResp.Body).Decode(&got)
		t.Fatalf("update status=%d body=%#v", putResp.StatusCode, got)
	}

	s.mu.RLock()
	updated := s.specs[id]
	s.mu.RUnlock()
	if updated == nil {
		t.Fatal("pipeline missing after update")
	}
	if updated.Source.Config["password"] != "source-secret-value" {
		t.Fatalf("source password overwritten: %#v", updated.Source.Config["password"])
	}
	if updated.Sink.Config["password"] != "sink-secret-value" {
		t.Fatalf("sink password overwritten: %#v", updated.Sink.Config["password"])
	}
	if updated.Sink.Config["format"] != "jsonl" {
		t.Fatalf("non-secret field not updated: %#v", updated.Sink.Config["format"])
	}
}
