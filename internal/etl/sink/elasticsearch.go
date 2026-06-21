package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
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
	maxRetries    int
	retryBaseMs   int
	chunkSize     int
	tlsSkipVerify bool
	client        *http.Client
	hostCounter   uint32
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
	if v, ok := config["hosts"]; ok {
		if hosts, ok := v.([]interface{}); ok {
			for _, h := range hosts {
				if hs, ok := h.(string); ok {
					s.hosts = append(s.hosts, strings.TrimRight(hs, "/"))
				}
			}
		}
	}
	if v, ok := config["host"]; ok {
		if vs, ok := v.(string); ok {
			s.hosts = append(s.hosts, strings.TrimRight(vs, "/"))
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
			s.index = vs
		}
	}
	if v, ok := config["id_column"]; ok {
		if vs, ok := v.(string); ok {
			s.idColumn = vs
		}
	}
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
	if s.chunkSize <= 0 {
		s.chunkSize = 500
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	if len(s.hosts) == 0 {
		s.hosts = []string{"http://localhost:9200"}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.tlsSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.client = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	return s, nil
}

func (s *ElasticsearchSink) Name() string { return s.name }

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

func (s *ElasticsearchSink) Write(ctx context.Context, records []core.Record) error {
	if len(records) == 0 {
		return nil
	}

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
					fmt.Printf("[WARN] elasticsearch sink: skip record (delete action marshal failed): doc_id=%s err=%v\n", docID, err)
					continue
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
					fmt.Printf("[WARN] elasticsearch sink: skip record (index action marshal failed): doc_id=%s err=%v\n", docID, err)
					continue
				}
				buf.Write(actionLine)
				buf.WriteByte('\n')

				docLine, err := json.Marshal(rec.Data)
				if err != nil {
					fmt.Printf("[WARN] elasticsearch sink: skip record (doc marshal failed): doc_id=%s err=%v\n", docID, err)
					// Remove the action line we just wrote so ES bulk NDJSON stays paired.
					buf.Truncate(buf.Len() - len(actionLine) - 1)
					continue
				}
				buf.Write(docLine)
				buf.WriteByte('\n')
			}
		}

		if buf.Len() > 0 {
			if err := s.bulkWithRetry(ctx, buf.Bytes()); err != nil {
				return err
			}
		}
	}

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
		if json.Unmarshal(respBody, &result) == nil {
			if errs, ok := result["errors"]; ok {
				if hasErrors, ok := errs.(bool); ok && hasErrors {
					summary := summarizeBulkErrors(result)
					if summary == "" {
						summary = responseSnippet(respBody)
					}
					lastErr = fmt.Errorf("elasticsearch bulk has errors: %s", summary)
					continue
				}
			}
		}

		return nil
	}
	return fmt.Errorf("elasticsearch bulk failed after %d retries: %w", s.maxRetries+1, lastErr)
}

// summarizeBulkErrors walks the ES bulk response "items" array and collects
// per-document error details (_id, type, reason). Returns "" if no per-item
// details could be extracted.
func summarizeBulkErrors(result map[string]any) string {
	itemsRaw, ok := result["items"]
	if !ok {
		return ""
	}
	items, ok := itemsRaw.([]any)
	if !ok {
		return ""
	}
	var details []string
	for _, itRaw := range items {
		it, ok := itRaw.(map[string]any)
		if !ok {
			continue
		}
		// Each item is keyed by the action name: "index", "delete", "create", "update".
		for _, entry := range it {
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
			if docID != "" {
				details = append(details, fmt.Sprintf("id=%s status=%d type=%s reason=%s", docID, int(status), errType, reason))
			} else {
				details = append(details, fmt.Sprintf("status=%d type=%s reason=%s", int(status), errType, reason))
			}
		}
	}
	if len(details) == 0 {
		return ""
	}
	summary := strings.Join(details, "; ")
	if len(details) > 20 {
		summary = strings.Join(details[:20], "; ") + fmt.Sprintf("; ...and %d more", len(details)-20)
	}
	return summary
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
