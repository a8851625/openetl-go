package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSink("elasticsearch", func(config map[string]any) (core.Sink, error) {
		return NewElasticsearchSink(config)
	})
	registry.RegisterSink("es", func(config map[string]any) (core.Sink, error) {
		return NewElasticsearchSink(config)
	})
}

type ElasticsearchSink struct {
	name          string
	hosts         []string
	username      string
	password      string
	index         string
	idColumn      string
	mappingTypes  map[string]string
	maxRetries    int
	retryBaseMs   int
	chunkSize     int
	tlsSkipVerify bool
	client        *http.Client
	hostCounter   uint32
	sinkCounters  // P4-20: per-sink write metrics (SK-4)
}

func NewElasticsearchSink(config map[string]any) (*ElasticsearchSink, error) {
	s := &ElasticsearchSink{
		name:        "elasticsearch",
		idColumn:    "id",
		maxRetries:  3,
		retryBaseMs: 500,
		chunkSize:   500,
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	for _, host := range stringSliceConfig(config, "hosts") {
		host = strings.TrimSpace(host)
		if host != "" {
			s.hosts = append(s.hosts, strings.TrimRight(host, "/"))
		}
	}
	if v, ok := config["host"]; ok {
		if vs, ok := v.(string); ok {
			vs = strings.TrimSpace(vs)
			if vs != "" {
				s.hosts = append(s.hosts, strings.TrimRight(vs, "/"))
			}
		}
	}
	if v, ok := config["username"]; ok {
		if vs, ok := v.(string); ok {
			s.username = vs
		}
	}
	if v, ok := config["password"]; ok {
		if vs, ok := v.(string); ok {
			s.password = vs
		}
	}
	if v, ok := config["index"]; ok {
		if vs, ok := v.(string); ok {
			s.index = strings.TrimSpace(vs)
		}
	}
	if v, ok := config["id_column"]; ok {
		if vs, ok := v.(string); ok {
			s.idColumn = vs
		}
	}
	s.mappingTypes = parseESMappingTypes(config)
	if v, ok := config["max_retries"]; ok {
		switch mr := v.(type) {
		case int:
			s.maxRetries = mr
		case float64:
			s.maxRetries = int(mr)
		}
	}
	if v, ok := config["retry_base_ms"]; ok {
		switch rb := v.(type) {
		case int:
			s.retryBaseMs = rb
		case float64:
			s.retryBaseMs = int(rb)
		}
	}
	if v, ok := config["chunk_size"]; ok {
		switch cs := v.(type) {
		case int:
			s.chunkSize = cs
		case float64:
			s.chunkSize = int(cs)
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	if err := s.validateConfig(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.tlsSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.client = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	return s, nil
}

func (s *ElasticsearchSink) validateConfig() error {
	if len(s.hosts) == 0 {
		return fmt.Errorf("elasticsearch sink hosts are required")
	}
	if s.index == "" {
		return fmt.Errorf("elasticsearch sink index is required")
	}
	if s.chunkSize <= 0 {
		return fmt.Errorf("elasticsearch sink chunk_size must be > 0")
	}
	if s.maxRetries < 0 {
		return fmt.Errorf("elasticsearch sink max_retries must be >= 0")
	}
	if s.retryBaseMs < 0 {
		return fmt.Errorf("elasticsearch sink retry_base_ms must be >= 0")
	}
	return nil
}

func (s *ElasticsearchSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *ElasticsearchSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *ElasticsearchSink) Open(ctx context.Context) error {
	for _, host := range s.hosts {
		req, err := http.NewRequestWithContext(ctx, "GET", host+"/_cluster/health", nil)
		if err != nil {
			continue
		}
		if s.username != "" {
			req.SetBasicAuth(s.username, s.password)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
		return nil
	}
	return fmt.Errorf("connect elasticsearch: no reachable hosts from %v", s.hosts)
}

func (s *ElasticsearchSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	mappingTypes := s.mappingTypes
	if len(mappingTypes) == 0 {
		var err error
		mappingTypes, err = s.fetchRemoteMappingTypes(ctx)
		if err != nil {
			return err
		}
	}
	if len(mappingTypes) == 0 {
		return nil
	}

	var incompatible []string
	for _, col := range schema.Columns {
		targetType, ok := mappingTypes[col.Name]
		if !ok {
			targetType, ok = mappingTypes[strings.ToLower(col.Name)]
		}
		if !ok || targetType == "" {
			continue
		}
		if !esTypeCompatible(col.DataType, targetType) {
			incompatible = append(incompatible, fmt.Sprintf("%s source=%s target=%s", col.Name, col.DataType, targetType))
		}
	}
	if len(incompatible) > 0 {
		return fmt.Errorf("schema validation failed for target: incompatible target column types [%s]", strings.Join(incompatible, "; "))
	}
	return nil
}

func parseESMappingTypes(config map[string]any) map[string]string {
	for _, key := range []string{"properties", "mappings", "mapping"} {
		if raw, ok := config[key]; ok {
			if props := extractESMappingProperties(raw); len(props) > 0 {
				return props
			}
		}
	}
	return nil
}

func extractESMappingProperties(raw any) map[string]string {
	m, ok := raw.(map[string]any)
	if !ok {
		if ms, ok := raw.(map[string]string); ok {
			out := make(map[string]string, len(ms))
			for k, v := range ms {
				out[k] = strings.ToLower(strings.TrimSpace(v))
				out[strings.ToLower(k)] = strings.ToLower(strings.TrimSpace(v))
			}
			return out
		}
		return nil
	}
	if propsRaw, ok := m["properties"]; ok {
		return extractESMappingProperties(propsRaw)
	}
	out := map[string]string{}
	for field, value := range m {
		switch v := value.(type) {
		case string:
			typ := strings.ToLower(strings.TrimSpace(v))
			if typ != "" {
				out[field] = typ
				out[strings.ToLower(field)] = typ
			}
		case map[string]any:
			typ, _ := v["type"].(string)
			typ = strings.ToLower(strings.TrimSpace(typ))
			if typ != "" {
				out[field] = typ
				out[strings.ToLower(field)] = typ
			}
		}
	}
	return out
}

func (s *ElasticsearchSink) fetchRemoteMappingTypes(ctx context.Context) (map[string]string, error) {
	if s.index == "" {
		return nil, nil
	}
	var lastErr error
	for _, host := range s.hosts {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, host+"/"+s.index+"/_mapping", nil)
		if err != nil {
			lastErr = err
			continue
		}
		if s.username != "" {
			req.SetBasicAuth(s.username, s.password)
		}
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		if resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("elasticsearch mapping status %d: %s", resp.StatusCode, responseSnippet(body))
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("parse elasticsearch mapping response: %w", err)
		}
		if props := propertiesFromESMappingResponse(parsed, s.index); len(props) > 0 {
			return props, nil
		}
		return nil, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("read elasticsearch mapping for index %q: %w", s.index, lastErr)
	}
	return nil, nil
}

func propertiesFromESMappingResponse(resp map[string]any, index string) map[string]string {
	if len(resp) == 0 {
		return nil
	}
	if raw, ok := resp[index]; ok {
		if idx, ok := raw.(map[string]any); ok {
			return extractESMappingProperties(idx["mappings"])
		}
	}
	for _, raw := range resp {
		if idx, ok := raw.(map[string]any); ok {
			if props := extractESMappingProperties(idx["mappings"]); len(props) > 0 {
				return props
			}
		}
	}
	return extractESMappingProperties(resp)
}

func esTypeCompatible(sourceType, targetType string) bool {
	src := strings.ToLower(strings.TrimSpace(sourceType))
	tgt := strings.ToLower(strings.TrimSpace(targetType))
	if src == "" || tgt == "" || tgt == "object" || tgt == "nested" {
		return true
	}
	if strings.Contains(src, "(") {
		src = src[:strings.Index(src, "(")]
	}
	switch {
	case strings.Contains(src, "bool"):
		return tgt == "boolean"
	case strings.Contains(src, "int") || strings.Contains(src, "bigint") || strings.Contains(src, "smallint") || strings.Contains(src, "tinyint") || strings.Contains(src, "long"):
		return tgt == "byte" || tgt == "short" || tgt == "integer" || tgt == "long" || tgt == "unsigned_long" || tgt == "float" || tgt == "half_float" || tgt == "double" || tgt == "scaled_float"
	case strings.Contains(src, "decimal") || strings.Contains(src, "numeric") || strings.Contains(src, "float") || strings.Contains(src, "double") || strings.Contains(src, "real"):
		return tgt == "float" || tgt == "half_float" || tgt == "double" || tgt == "scaled_float"
	case strings.Contains(src, "date") || strings.Contains(src, "time"):
		return tgt == "date" || tgt == "date_nanos" || tgt == "keyword" || tgt == "text"
	case strings.Contains(src, "char") || strings.Contains(src, "text") || strings.Contains(src, "string") || strings.Contains(src, "json"):
		return tgt == "keyword" || tgt == "text" || tgt == "wildcard" || tgt == "constant_keyword" || tgt == "match_only_text" || tgt == "semantic_text"
	default:
		return true
	}
}

func (s *ElasticsearchSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}() // P5-12: count write failures
	if len(records) == 0 {
		return nil
	}
	start := time.Now()

	// Chunk to avoid exceeding ES http.max_content_length (~100MB default).
	for offset := 0; offset < len(records); offset += s.chunkSize {
		end := offset + s.chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[offset:end]

		var buf bytes.Buffer
		for _, rec := range chunk {
			indexName := s.index
			if indexName == "" && rec.Metadata.Table != "" {
				indexName = strings.ToLower(rec.Metadata.Table)
			}
			if indexName == "" {
				indexName = strings.ToLower(s.name)
			}

			docID := ""
			if v, ok := rec.Data[s.idColumn]; ok && v != nil {
				docID = fmt.Sprintf("%v", v)
			}
			// When no id_column is configured or the value is missing, derive
			// a deterministic _id from source metadata so that replay does not
			// create duplicate documents under ES auto-generated IDs.
			if docID == "" {
				docID = deriveESDocID(rec)
			}

			// Route by operation: DELETE → delete action, others → index
			if rec.Operation == core.OpDelete {
				if docID == "" {
					return fmt.Errorf("elasticsearch delete requires id column %q", s.idColumn)
				}
				action := map[string]any{"delete": map[string]any{
					"_index": indexName,
				}}
				action["delete"].(map[string]any)["_id"] = docID
				actionLine, err := json.Marshal(action)
				if err != nil {
					return fmt.Errorf("elasticsearch marshal delete action (doc_id=%s): %w", docID, err)
				}
				buf.Write(actionLine)
				buf.WriteByte('\n')
			} else {
				action := map[string]any{"index": map[string]any{
					"_index": indexName,
				}}
				if docID != "" {
					action["index"].(map[string]any)["_id"] = docID
				}

				actionLine, err := json.Marshal(action)
				if err != nil {
					return fmt.Errorf("elasticsearch marshal index action (doc_id=%s): %w", docID, err)
				}
				buf.Write(actionLine)
				buf.WriteByte('\n')

				docLine, err := json.Marshal(rec.Data)
				if err != nil {
					return fmt.Errorf("elasticsearch marshal document (doc_id=%s): %w", docID, err)
				}
				buf.Write(docLine)
				buf.WriteByte('\n')
			}
		}

		if buf.Len() > 0 {
			if err := s.bulkWithRetry(ctx, buf.Bytes()); err != nil {
				var itemErr *elasticsearchBulkItemError
				if errors.As(err, &itemErr) {
					return itemErr.withOffset(offset)
				}
				return err
			}
		}
	}

	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *ElasticsearchSink) bulkWithRetry(ctx context.Context, body []byte) error {
	var lastErr error
	// pendingDelay is set when the server sends Retry-After on a 429; it
	// overrides the exponential backoff for the next attempt.
	pendingDelay := time.Duration(0)
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			delay := pendingDelay
			if delay <= 0 {
				delay = time.Duration(s.retryBaseMs<<(attempt-1)) * time.Millisecond
			}
			pendingDelay = 0
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		host := s.nextHost()
		url := host + "/_bulk"
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
		if err != nil {
			lastErr = fmt.Errorf("create bulk request: %w", err)
			continue
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		if s.username != "" {
			req.SetBasicAuth(s.username, s.password)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("bulk request: %w", err)
			continue
		}

		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		retryAfter := resp.Header.Get("Retry-After")
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("elasticsearch status %d: %s", resp.StatusCode, responseSnippet(respBody))
			// If the server told us how long to wait (Retry-After, seconds),
			// honor it for the next attempt instead of exponential backoff.
			if resp.StatusCode == http.StatusTooManyRequests && retryAfter != "" {
				if secs, perr := strconv.Atoi(retryAfter); perr == nil && secs > 0 {
					pendingDelay = time.Duration(secs) * time.Second
				}
			}
			continue
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("elasticsearch bulk failed: %d %s", resp.StatusCode, responseSnippet(respBody))
		}

		var result map[string]any
		if json.Unmarshal(respBody, &result) != nil {
			// 2xx but the body is not valid JSON (e.g. an HTML proxy/error page).
			// The commit state is unknown — never treat this as success, or a
			// failed batch would advance the checkpoint (silent data loss).
			return fmt.Errorf("elasticsearch bulk: unparseable response body (status %d): %s",
				resp.StatusCode, responseSnippet(respBody))
		}
		if errs, ok := result["errors"]; ok {
			if hasErrors, ok := errs.(bool); ok && hasErrors {
				failures := collectBulkItemFailures(result)
				if len(failures) > 0 {
					lastErr = &elasticsearchBulkItemError{failures: failures}
				} else {
					summary := summarizeBulkErrors(result)
					if summary == "" {
						summary = responseSnippet(respBody)
					}
					lastErr = fmt.Errorf("elasticsearch bulk has errors: %s", summary)
				}
				continue
			}
		}

		return nil
	}
	return fmt.Errorf("elasticsearch bulk failed after %d retries: %w", s.maxRetries+1, lastErr)
}

type elasticsearchBulkItemFailure struct {
	Index   int
	Action  string
	Status  int
	DocID   string
	ErrType string
	Reason  string
}

type elasticsearchBulkItemError struct {
	failures []elasticsearchBulkItemFailure
}

func (e *elasticsearchBulkItemError) Error() string {
	if e == nil || len(e.failures) == 0 {
		return "elasticsearch bulk has item errors"
	}
	return "elasticsearch bulk has item errors: " + summarizeBulkItemFailures(e.failures)
}

func (e *elasticsearchBulkItemError) FailedRecordIndices() []int {
	if e == nil {
		return nil
	}
	indices := make([]int, len(e.failures))
	for i, failure := range e.failures {
		indices[i] = failure.Index
	}
	return indices
}

func (e *elasticsearchBulkItemError) ErrorForRecord(index int) error {
	if e == nil {
		return nil
	}
	for _, failure := range e.failures {
		if failure.Index != index {
			continue
		}
		return core.ClassifiedError{
			Class: classifyESBulkItemFailure(failure),
			Err:   fmt.Errorf("elasticsearch bulk item failed: %s", formatBulkItemFailure(failure)),
		}
	}
	return nil
}

func (e *elasticsearchBulkItemError) withOffset(offset int) *elasticsearchBulkItemError {
	if e == nil || offset == 0 {
		return e
	}
	failures := make([]elasticsearchBulkItemFailure, len(e.failures))
	copy(failures, e.failures)
	for i := range failures {
		failures[i].Index += offset
	}
	return &elasticsearchBulkItemError{failures: failures}
}

func collectBulkItemFailures(result map[string]any) []elasticsearchBulkItemFailure {
	itemsRaw, ok := result["items"]
	if !ok {
		return nil
	}
	items, ok := itemsRaw.([]any)
	if !ok {
		return nil
	}
	var failures []elasticsearchBulkItemFailure
	for idx, itRaw := range items {
		it, ok := itRaw.(map[string]any)
		if !ok {
			continue
		}
		// Each item is keyed by the action name: "index", "delete", "create", "update".
		for action, entry := range it {
			entryMap, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			status, _ := entryMap["status"].(float64)
			errObj, ok := entryMap["error"]
			if !ok {
				continue
			}
			if status != 0 && status < 400 {
				continue
			}
			docID, _ := entryMap["_id"].(string)
			errType, reason := decodeBulkError(errObj)
			failures = append(failures, elasticsearchBulkItemFailure{
				Index:   idx,
				Action:  action,
				Status:  int(status),
				DocID:   docID,
				ErrType: errType,
				Reason:  reason,
			})
		}
	}
	return failures
}

// summarizeBulkErrors walks the ES bulk response "items" array and collects
// per-document error details (_id, type, reason). Returns "" if no per-item
// details could be extracted.
func summarizeBulkErrors(result map[string]any) string {
	return summarizeBulkItemFailures(collectBulkItemFailures(result))
}

func summarizeBulkItemFailures(failures []elasticsearchBulkItemFailure) string {
	if len(failures) == 0 {
		return ""
	}
	details := make([]string, 0, len(failures))
	for _, failure := range failures {
		details = append(details, formatBulkItemFailure(failure))
	}
	summary := strings.Join(details, "; ")
	if len(details) > 20 {
		summary = strings.Join(details[:20], "; ") + fmt.Sprintf("; ...and %d more", len(details)-20)
	}
	return summary
}

func formatBulkItemFailure(failure elasticsearchBulkItemFailure) string {
	parts := []string{fmt.Sprintf("item=%d", failure.Index)}
	if failure.Action != "" {
		parts = append(parts, "action="+failure.Action)
	}
	if failure.DocID != "" {
		parts = append(parts, "id="+failure.DocID)
	}
	if failure.Status != 0 {
		parts = append(parts, fmt.Sprintf("status=%d", failure.Status))
	}
	if failure.ErrType != "" {
		parts = append(parts, "type="+failure.ErrType)
	}
	if failure.Reason != "" {
		parts = append(parts, "reason="+failure.Reason)
	}
	return strings.Join(parts, " ")
}

func classifyESBulkItemFailure(failure elasticsearchBulkItemFailure) core.ErrorClass {
	if failure.Status == http.StatusUnauthorized || failure.Status == http.StatusForbidden {
		return core.ErrorClassAuth
	}
	if failure.Status == http.StatusTooManyRequests || failure.Status >= 500 {
		return core.ErrorClassTransient
	}
	msg := strings.ToLower(failure.ErrType + " " + failure.Reason)
	switch {
	case strings.Contains(msg, "mapper") || strings.Contains(msg, "mapping") || strings.Contains(msg, "schema") || strings.Contains(msg, "type"):
		return core.ErrorClassSchema
	case failure.Status >= 400 && failure.Status < 500:
		return core.ErrorClassData
	default:
		return core.ErrorClassUnknown
	}
}

// decodeBulkError extracts (type, reason) from an ES bulk item error object.
// ES error objects look like: {"type":"mapper_parsing_exception","reason":"..."}.
func decodeBulkError(errObj any) (errType, reason string) {
	if m, ok := errObj.(map[string]any); ok {
		if t, ok := m["type"].(string); ok {
			errType = t
		}
		if r, ok := m["reason"].(string); ok {
			reason = r
		}
		return
	}
	if s, ok := errObj.(string); ok {
		reason = s
	}
	return
}

func (s *ElasticsearchSink) nextHost() string {
	n := atomic.AddUint32(&s.hostCounter, 1)
	return s.hosts[int(n)%len(s.hosts)]
}

func responseSnippet(body []byte) string {
	if len(body) > 500 {
		body = body[:500]
	}
	return string(body)
}

// deriveESDocID produces a deterministic document _id from source metadata
// (table + offset/partition/binlog) so that replay rewrites the same doc
// instead of creating duplicates under ES auto-generated IDs.
func deriveESDocID(rec core.Record) string {
	var key string
	if rec.Metadata.Table != "" {
		key = rec.Metadata.Table + "|"
	}
	if rec.Metadata.Offset > 0 {
		key += fmt.Sprintf("off=%d", rec.Metadata.Offset)
	} else if rec.Metadata.Partition != 0 || rec.Metadata.Offset != 0 {
		key += fmt.Sprintf("p=%d:o=%d", rec.Metadata.Partition, rec.Metadata.Offset)
	} else if rec.Metadata.BinlogFile != "" {
		key += fmt.Sprintf("b=%s:%d", rec.Metadata.BinlogFile, rec.Metadata.BinlogPos)
	} else if rec.Metadata.LSN != "" {
		key += "lsn=" + rec.Metadata.LSN
	} else {
		// No usable source metadata: fall back to hashing the data map so
		// identical content at least collapses, though ordering is not encoded.
		key = fmt.Sprintf("data=%v", rec.Data)
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

func (s *ElasticsearchSink) Close() error { return nil }
