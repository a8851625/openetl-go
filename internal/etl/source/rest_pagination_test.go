package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// readAll drains a source and returns the record count.
func drainSource(t *testing.T, src *RestSource) int {
	t.Helper()
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	count := 0
	for i := 0; i < 100; i++ {
		rec, err := reader.Read(context.Background())
		if err != nil {
			if err.Error() == "EOF" || err == context.Canceled {
				break
			}
			break
		}
		_ = rec
		count++
	}
	return count
}

// TestSalesforceStyleNextURLPagination proves the URL-paged stream follows the
// server-provided nextRecordsUrl (instance-relative) instead of offset paging,
// which Salesforce rejects beyond the first 2000 rows.
func TestSalesforceStyleNextURLPagination(t *testing.T) {
	var requests []string
	var serverURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/services/data/v59.0/query":
			_, _ = w.Write([]byte(`{"totalSize":3,"done":false,"records":[{"Id":"001"},{"Id":"002"}],"nextRecordsUrl":"/services/data/v59.0/query/01g-2000"}`))
		case "/services/data/v59.0/query/01g-2000":
			_, _ = w.Write([]byte(`{"totalSize":3,"done":true,"records":[{"Id":"003"}]}`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()
	serverURL = ts.URL

	cfg := map[string]any{
		"name":           "salesforce",
		"url":            serverURL + "/services/data/v59.0/query?q=SELECT+Id+FROM+Contact",
		"method":         "GET",
		"pagination":     "cursor",
		"next_url_field": "nextRecordsUrl",
		"result_key":     "records",
		"auth":           "none",
	}
	src, err := NewRestSource(cfg)
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	if got := drainSource(t, src); got != 3 {
		t.Fatalf("records = %d, want 3 (requests: %v)", got, requests)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %v, want exactly 2 (the next URL must be followed)", requests)
	}
	if !strings.Contains(requests[1], "/query/01g-2000") {
		t.Fatalf("second request = %q, want the nextRecordsUrl path", requests[1])
	}
	if strings.Contains(requests[1], "offset=") {
		t.Fatalf("second request must not use offset paging: %q", requests[1])
	}
}

// TestRestCursorUnchangedStopsStream guards against a provider echoing the same
// cursor, which would otherwise re-fetch the same page forever.
func TestRestCursorUnchangedStopsStream(t *testing.T) {
	var calls int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"id":1}],"next_cursor":"same"}`))
	}))
	defer ts.Close()

	src, err := NewRestSource(map[string]any{
		"name":         "rest_source",
		"url":          ts.URL,
		"method":       "GET",
		"pagination":   "cursor",
		"cursor_param": "cursor",
		"cursor_field": "next_cursor",
		"result_key":   "items",
		"auth":         "none",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	got := drainSource(t, src)
	if got != 2 {
		t.Fatalf("records = %d, want 2 (one per page, then stop)", got)
	}
	if n := atomic.LoadInt64(&calls); n != 2 {
		t.Fatalf("requests = %d, want 2 (a repeated cursor must stop the stream)", n)
	}
}

// TestRestRetriesRateLimitedForbidden covers GitHub's primary rate limit, which
// answers 403 + x-ratelimit-remaining: 0 instead of 429.
func TestRestRetriesRateLimitedForbidden(t *testing.T) {
	var calls int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt64(&calls, 1) == 1 {
			w.Header().Set("x-ratelimit-remaining", "0")
			w.Header().Set("retry-after", "0")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1},{"id":2}]`))
	}))
	defer ts.Close()

	src, err := NewRestSource(map[string]any{
		"name":        "github",
		"url":         ts.URL,
		"method":      "GET",
		"auth":        "none",
		"max_retries": 2,
		"result_key":  "",
	})
	if err != nil {
		t.Fatalf("NewRestSource: %v", err)
	}
	if got := drainSource(t, src); got != 2 {
		t.Fatalf("records = %d, want 2 after the rate-limit retry", got)
	}
	if n := atomic.LoadInt64(&calls); n != 2 {
		t.Fatalf("requests = %d, want 2 (the 403 rate limit must be retried)", n)
	}
}

func TestRateLimitDelayFromHeaders(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("retry-after", "2")
	if got := rateLimitDelay(resp); got != 2*time.Second {
		t.Fatalf("retry-after delay = %v, want 2s", got)
	}
	resp2 := &http.Response{Header: http.Header{}}
	resp2.Header.Set("x-ratelimit-reset", time.Now().Add(90*time.Second).Format("1136239445"))
	if got := rateLimitDelay(resp2); got <= 0 {
		t.Fatalf("reset delay = %v, want a positive wait", got)
	}
	if got := rateLimitDelay(&http.Response{Header: http.Header{}}); got != 0 {
		t.Fatalf("no headers delay = %v, want 0", got)
	}
}

func TestIsRateLimitedResponse(t *testing.T) {
	mk := func(status int, headers map[string]string) *http.Response {
		h := http.Header{}
		for k, v := range headers {
			h.Set(k, v)
		}
		return &http.Response{StatusCode: status, Header: h}
	}
	if !isRateLimitedResponse(mk(http.StatusForbidden, map[string]string{"x-ratelimit-remaining": "0"})) {
		t.Error("403 with x-ratelimit-remaining: 0 must be treated as rate limited")
	}
	if !isRateLimitedResponse(mk(http.StatusForbidden, map[string]string{"retry-after": "5"})) {
		t.Error("403 with retry-after must be treated as rate limited")
	}
	if isRateLimitedResponse(mk(http.StatusForbidden, map[string]string{"x-ratelimit-remaining": "12"})) {
		t.Error("a plain 403 must stay terminal")
	}
	if isRateLimitedResponse(mk(http.StatusNotFound, nil)) {
		t.Error("404 must not be retried")
	}
}

func TestResolveNextURL(t *testing.T) {
	cases := []struct{ base, next, want string }{
		{"https://acme.my.salesforce.com/services/data/v59.0/query", "/services/data/v59.0/query/01g-2000", "https://acme.my.salesforce.com/services/data/v59.0/query/01g-2000"},
		{"https://api.example.com/v1/items", "https://api.example.com/v1/items?page=2", "https://api.example.com/v1/items?page=2"},
		{"https://api.example.com/v1/items", "", ""},
	}
	for _, tc := range cases {
		if got := resolveNextURL(tc.base, tc.next); got != tc.want {
			t.Errorf("resolveNextURL(%q, %q) = %q, want %q", tc.base, tc.next, got, tc.want)
		}
	}
}
