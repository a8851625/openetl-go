package source

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestRestSourceAPIKeyHeader(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-API-Key")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"id": 1}}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":            srv.URL,
		"auth":           "api_key",
		"api_key_header": "X-API-Key",
		"api_key_value":  "secret-key",
		"result_key":     "data",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len=%d, want 1", len(recs))
	}
	if gotKey != "secret-key" {
		t.Errorf("X-API-Key=%q, want secret-key", gotKey)
	}
}

func TestRestSourceAPIKeyQuery(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("api_key")
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"id": 1}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL,
		"auth":          "api_key",
		"api_key_query": "api_key",
		"api_key_value": "qk-123",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	if _, err := reader.ReadBatch(context.Background(), 10); err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if gotKey != "qk-123" {
		t.Errorf("api_key query=%q, want qk-123", gotKey)
	}
}

func TestRestSourceBearerAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"ok": true}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":   srv.URL,
		"auth":  "bearer",
		"token": "tok-abc",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	if _, err := reader.ReadBatch(context.Background(), 10); err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if gotAuth != "Bearer tok-abc" {
		t.Errorf("Authorization=%q", gotAuth)
	}
}

func TestRestSourceBasicAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode([]any{})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":      srv.URL,
		"auth":     "basic_auth",
		"username": "alice",
		"password": "secret",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	_, _ = reader.ReadBatch(context.Background(), 10)
	if gotAuth != "Basic YWxpY2U6c2VjcmV0" {
		t.Errorf("basic auth=%q", gotAuth)
	}
}

func TestRestSourceOAuth2(t *testing.T) {
	var tokenHits, dataHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenHits, 1)
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.PostForm.Get("grant_type") != "client_credentials" {
			t.Errorf("grant_type=%q", r.PostForm.Get("grant_type"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "oa-1", "expires_in": 3600})
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dataHits, 1)
		if r.Header.Get("Authorization") != "Bearer oa-1" {
			t.Errorf("Authorization=%q", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"id": 1}}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL + "/data",
		"auth":          "oauth2_client_credentials",
		"token_url":     srv.URL + "/token",
		"client_id":     "cid",
		"client_secret": "csec",
		"result_key":    "results",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len=%d", len(recs))
	}
	if tokenHits != 1 || dataHits != 1 {
		t.Fatalf("tokenHits=%d dataHits=%d", tokenHits, dataHits)
	}
}

func TestRestSourceOffsetPagination(t *testing.T) {
	var pages []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		off, _ := strconv.Atoi(r.URL.Query().Get("_offset"))
		pages = append(pages, off)
		limit, _ := strconv.Atoi(r.URL.Query().Get("_limit"))
		if limit == 0 {
			limit = 2
		}
		var items []any
		// 总共 5 条：offset 0,2,4
		for i := 0; i < limit && off+i < 5; i++ {
			items = append(items, map[string]any{"id": off + i})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"records":   items,
			"totalSize": 5,
		})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":         srv.URL,
		"pagination":  "offset",
		"page_param":  "_offset",
		"size_param":  "_limit",
		"page_size":   2,
		"result_key":  "records",
		"total_field": "totalSize",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	var all []coreRecordID
	for {
		rec, err := reader.Read(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		all = append(all, coreRecordID{ID: int(rec.Data["id"].(float64))})
	}
	if len(all) != 5 {
		t.Fatalf("got %d records, want 5; pages=%v", len(all), pages)
	}
	if len(pages) < 3 {
		t.Fatalf("pages fetched=%v, want at least 3", pages)
	}
}

type coreRecordID struct{ ID int }

func TestRestSourceCursorBodyPagination(t *testing.T) {
	var cursors []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := r.URL.Query().Get("cursor")
		cursors = append(cursors, c)
		if c == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":        []any{map[string]any{"id": "a"}},
				"next_cursor": "c2",
			})
			return
		}
		if c == "c2" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":        []any{map[string]any{"id": "b"}},
				"next_cursor": "",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":          srv.URL,
		"pagination":   "cursor",
		"cursor_param": "cursor",
		"cursor_field": "next_cursor",
		"result_key":   "data",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	var ids []string
	for {
		rec, err := reader.Read(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		ids = append(ids, rec.Data["id"].(string))
	}
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("ids=%v cursors=%v", ids, cursors)
	}
}

func TestRestSourceCursorHeaderPagination(t *testing.T) {
	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		if page == "" || page == "1" {
			w.Header().Set("Link", `<http://example/x?page=2>; rel="next"`)
			_ = json.NewEncoder(w).Encode([]any{map[string]any{"id": 1}})
			return
		}
		// 最后一页无 Link
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"id": 2}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL,
		"pagination":    "cursor",
		"cursor_param":  "page",
		"cursor_header": "Link",
		"page_size":     1,
		"size_param":    "per_page",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	var n int
	for {
		_, err := reader.Read(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("n=%d pages=%v", n, pages)
	}
}

func TestRestSourcePageTokenPagination(t *testing.T) {
	var tokens []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		tok, _ := body["start_cursor"].(string)
		tokens = append(tokens, tok)
		if tok == "" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results":     []any{map[string]any{"id": "p1"}},
				"next_cursor": "tok2",
				"has_more":    true,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":     []any{map[string]any{"id": "p2"}},
			"next_cursor": "",
			"has_more":    false,
		})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":         srv.URL,
		"method":      "POST",
		"body":        `{"page_size":100}`,
		"pagination":  "page_token",
		"token_param": "start_cursor",
		"token_field": "next_cursor",
		"result_key":  "results",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	var ids []string
	for {
		rec, err := reader.Read(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		ids = append(ids, rec.Data["id"].(string))
	}
	if len(ids) != 2 {
		t.Fatalf("ids=%v tokens=%v", ids, tokens)
	}
	if len(tokens) < 2 || tokens[1] != "tok2" {
		t.Fatalf("tokens=%v, want second=tok2", tokens)
	}
}

func TestRestSourceRetry429WithRetryAfter(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"ok": true}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL,
		"max_retries":   3,
		"retry_base_ms": 5,
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("len=%d", len(recs))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestRestSourceRetry5xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"k": "v"}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL,
		"max_retries":   3,
		"retry_base_ms": 5,
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 || attempts != 3 {
		t.Fatalf("len=%d attempts=%d", len(recs), attempts)
	}
}

func TestRestSourceNoRetryOn4xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":           srv.URL,
		"max_retries":   3,
		"retry_base_ms": 5,
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	_, err = reader.ReadBatch(context.Background(), 10)
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestRestSourceVariableExpansion(t *testing.T) {
	var gotPath, gotHeader, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHeader = r.Header.Get("X-Tenant")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_ = json.NewEncoder(w).Encode([]any{map[string]any{"ok": true}})
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":    srv.URL + "/v1/${context.tenant}/items",
		"method": "POST",
		"headers": map[string]any{
			"X-Tenant": "${tenant}",
		},
		"body": `{"tenant":"${context.tenant}"}`,
		"variables": map[string]any{
			"tenant":         "acme",
			"context.tenant": "acme",
		},
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	reader, _ := src.Open(context.Background(), nil)
	if _, err := reader.ReadBatch(context.Background(), 10); err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if gotPath != "/v1/acme/items" {
		t.Errorf("path=%q", gotPath)
	}
	if gotHeader != "acme" {
		t.Errorf("header=%q", gotHeader)
	}
	if gotBody != `{"tenant":"acme"}` {
		t.Errorf("body=%q", gotBody)
	}
}

func TestRestSourcePreflightProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer t" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	src, err := NewRestSource(map[string]any{
		"url":   srv.URL,
		"auth":  "bearer",
		"token": "t",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := src.PreflightProbe(ctx); err != nil {
		t.Fatalf("PreflightProbe: %v", err)
	}
}

func TestRestSourceValidation(t *testing.T) {
	cases := []map[string]any{
		{}, // missing url
		{"url": "http://x", "auth": "api_key"},
		{"url": "http://x", "auth": "bearer"},
		{"url": "http://x", "auth": "oauth2_client_credentials", "client_id": "c"},
		{"url": "http://x", "pagination": "weird"},
	}
	for i, cfg := range cases {
		if _, err := NewRestSource(cfg); err == nil {
			t.Fatalf("case %d: expected error for %#v", i, cfg)
		}
	}
}

func TestParseLinkHeaderCursor(t *testing.T) {
	got := parseLinkHeaderCursor(`<https://api.github.com/repos/o/r/issues?page=2>; rel="next", <https://api.github.com/repos/o/r/issues?page=5>; rel="last"`)
	if got != "2" {
		t.Fatalf("got %q, want 2", got)
	}
}
