package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("http", func(config map[string]any) (core.Source, error) {
		return NewHTTPSource(config)
	})
}

type HTTPSource struct {
	name        string
	url         string
	method      string
	headers     map[string]string
	body        string
	pagination  string
	pageParam   string
	sizeParam   string
	pageSize    int
	maxPages    int
	resultKey   string
	authType    string
	authToken   string
	authUser    string
	authPass    string
	// OAuth2 client_credentials
	oauth2TokenURL    string
	oauth2ClientID    string
	oauth2ClientSecret string
	oauth2TokenField  string
	oauth2HeaderFmt   string
	oauth2Scopes      string
	client      *http.Client
	maxRetries  int
	retryBaseMs int
	shardIndex  int
	shardTotal  int
	// cached OAuth2 token (managed by the reader on demand)
	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

func NewHTTPSource(config map[string]any) (*HTTPSource, error) {
	s := &HTTPSource{
		name:        "http",
		method:      "GET",
		pageSize:    100,
		maxPages:    0,
		client:      &http.Client{Timeout: 30 * time.Second},
		maxRetries:  3,
		retryBaseMs: 500,
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["url"]; ok {
		if vs, ok := v.(string); ok {
			s.url = vs
		}
	}
	if v, ok := config["method"]; ok {
		if vs, ok := v.(string); ok {
			s.method = vs
		}
	}
	if v, ok := config["headers"]; ok {
		if h, ok := v.(map[string]interface{}); ok {
			s.headers = make(map[string]string)
			for k, val := range h {
				if vs, ok := val.(string); ok {
					s.headers[k] = vs
				}
			}
		}
	}
	if v, ok := config["body"]; ok {
		if vs, ok := v.(string); ok {
			s.body = vs
		}
	}
	if v, ok := config["pagination"]; ok {
		if vs, ok := v.(string); ok {
			s.pagination = vs
		}
	}
	if v, ok := config["page_param"]; ok {
		s.pageParam = v.(string)
	}
	if v, ok := config["size_param"]; ok {
		s.sizeParam = v.(string)
	}
	if v, ok := config["page_size"]; ok {
		switch ps := v.(type) {
		case int:
			s.pageSize = ps
		case float64:
			s.pageSize = int(ps)
		}
	}
	if v, ok := config["max_pages"]; ok {
		switch mp := v.(type) {
		case int:
			s.maxPages = mp
		case float64:
			s.maxPages = int(mp)
		}
	}
	if v, ok := config["result_key"]; ok {
		if vs, ok := v.(string); ok {
			s.resultKey = vs
		}
	}
	if v, ok := config["auth_type"]; ok {
		if vs, ok := v.(string); ok {
			s.authType = vs
		}
	}
	if v, ok := config["auth_token"]; ok {
		if vs, ok := v.(string); ok {
			s.authToken = vs
		}
	}
	if v, ok := config["auth_user"]; ok {
		if vs, ok := v.(string); ok {
			s.authUser = vs
		}
	}
	if v, ok := config["auth_pass"]; ok {
		if vs, ok := v.(string); ok {
			s.authPass = vs
		}
	}
	// OAuth2 client_credentials fields
	if v, ok := config["oauth2_token_url"]; ok {
		s.oauth2TokenURL, _ = v.(string)
	}
	if v, ok := config["oauth2_client_id"]; ok {
		s.oauth2ClientID, _ = v.(string)
	}
	if v, ok := config["oauth2_client_secret"]; ok {
		s.oauth2ClientSecret, _ = v.(string)
	}
	if v, ok := config["oauth2_token_field"]; ok {
		s.oauth2TokenField, _ = v.(string)
	}
	if v, ok := config["oauth2_header_format"]; ok {
		s.oauth2HeaderFmt, _ = v.(string)
	}
	if v, ok := config["oauth2_scopes"]; ok {
		s.oauth2Scopes, _ = v.(string)
	}
	if s.oauth2TokenField == "" {
		s.oauth2TokenField = "access_token"
	}
	if s.oauth2HeaderFmt == "" {
		s.oauth2HeaderFmt = "Bearer %s"
	}
	if s.authType == "oauth2_client_credentials" {
		if s.oauth2TokenURL == "" || s.oauth2ClientID == "" || s.oauth2ClientSecret == "" {
			return nil, fmt.Errorf("http source auth_type=oauth2_client_credentials requires oauth2_token_url, oauth2_client_id, oauth2_client_secret")
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
	s.shardIndex, s.shardTotal = readShardConfig(config)
	return s, nil
}

func (s *HTTPSource) Name() string { return s.name }

// fetchOAuth2Token implements the OAuth2 client_credentials grant:
// POST {token_url} with form body grant_type=client_credentials&scope=...&client_id=...&client_secret=...
// Parses the configured `oauth2_token_field` (default access_token) and
// `expires_in` for proactive refresh.
func (s *HTTPSource) fetchOAuth2Token(ctx context.Context) error {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	if s.oauth2Scopes != "" {
		form.Set("scope", s.oauth2Scopes)
	}
	form.Set("client_id", s.oauth2ClientID)
	form.Set("client_secret", s.oauth2ClientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.oauth2TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("oauth2 token fetch: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("oauth2 token HTTP %d: %s", resp.StatusCode, string(body))
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("oauth2 token decode: %w", err)
	}
	if errStr, ok := parsed["error"].(string); ok && errStr != "" {
		desc, _ := parsed["error_description"].(string)
		return fmt.Errorf("oauth2 token error: %s %s", errStr, desc)
	}
	token, _ := parsed[s.oauth2TokenField].(string)
	if token == "" {
		return fmt.Errorf("oauth2 token: field %q missing in response", s.oauth2TokenField)
	}
	expiry := time.Now().Add(time.Hour)
	if expIn, ok := parsed["expires_in"].(float64); ok && expIn > 0 {
		expiry = time.Now().Add(time.Duration(expIn)*time.Second - 60*time.Second)
	}
	s.tokenMu.Lock()
	s.accessToken = token
	s.tokenExpiry = expiry
	s.tokenMu.Unlock()
	return nil
}

// ensureOAuth2Token returns a valid token, refreshing when missing or close to expiry.
func (s *HTTPSource) ensureOAuth2Token(ctx context.Context) (string, error) {
	s.tokenMu.Lock()
	if s.accessToken != "" && time.Now().Before(s.tokenExpiry) {
		tok := s.accessToken
		s.tokenMu.Unlock()
		return tok, nil
	}
	s.tokenMu.Unlock()
	if err := s.fetchOAuth2Token(ctx); err != nil {
		return "", err
	}
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	return s.accessToken, nil
}

func (s *HTTPSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	reader := &httpReader{
		source: s,
		page:   0,
		done:   false,
	}
	// Sharding: shard i starts at page (shardIndex+1) and advances by shardTotal.
	// E.g. shard_total=3: shard 0 → pages 1,4,7,10...; shard 1 → pages 2,5,8...
	if s.shardTotal > 1 {
		reader.page = s.shardIndex // next fetchPage does page++ → starts at shardIndex+1
	}
	// Restore checkpoint: resume from the page AFTER the last committed one
	if cp != nil && len(cp.Position) > 0 && string(cp.Position) != "null" {
		var pos httpPosition
		if err := json.Unmarshal(cp.Position, &pos); err == nil && pos.Page > 0 {
			reader.page = pos.Page          // resume from next page on next fetchPage
			reader.committedPage = pos.Page // last fully consumed page
		}
	}
	return reader, nil
}

type httpReader struct {
	source        *HTTPSource
	page          int
	committedPage int
	buffer        []core.Record
	done          bool
	fetchedAt     time.Time
}

func (r *httpReader) Read(ctx context.Context) (core.Record, error) {
	if len(r.buffer) == 0 {
		if r.done {
			return core.Record{}, io.EOF
		}
		recs, err := r.fetchPage(ctx)
		if err != nil {
			return core.Record{}, err
		}
		if len(recs) == 0 {
			r.done = true
			return core.Record{}, io.EOF
		}
		r.buffer = recs
	}
	rec := r.buffer[0]
	r.buffer = r.buffer[1:]
	if len(r.buffer) == 0 {
		// Page fully consumed: safe to commit this page as resume point.
		r.committedPage = r.page
	}
	return rec, nil
}

func (r *httpReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	if len(r.buffer) == 0 && !r.done {
		recs, err := r.fetchPage(ctx)
		if err != nil {
			return nil, err
		}
		r.buffer = recs
	}
	if n <= 0 || n >= len(r.buffer) {
		result := r.buffer
		r.buffer = nil
		r.committedPage = r.page
		return result, nil
	}
	result := r.buffer[:n]
	r.buffer = r.buffer[n:]
	if len(r.buffer) == 0 {
		r.committedPage = r.page
	}
	return result, nil
}

func (r *httpReader) fetchPage(ctx context.Context) ([]core.Record, error) {
	pageURL := r.source.url
	if r.source.pagination == "page" || r.source.pagination == "" {
		// Sharded advance: step by shardTotal instead of 1
		step := 1
		if r.source.shardTotal > 1 {
			step = r.source.shardTotal
		}
		r.page += step
		if r.source.maxPages > 0 && r.page > r.source.maxPages {
			r.done = true
			return nil, nil
		}
		pageURL = applyPageParams(pageURL, r.source.pageParam, r.page, r.source.sizeParam, r.source.pageSize)
	}

	items, err := r.fetchWithRetry(ctx, pageURL)
	if err != nil {
		return nil, err
	}

	records := make([]core.Record, 0, len(items))
	ts := time.Now()
	for _, item := range items {
		if data, ok := item.(map[string]any); ok {
			records = append(records, core.Record{
				Operation: core.OpInsert,
				Data:      data,
				Metadata: core.Metadata{
					Source:    r.source.name,
					Table:     r.source.name,
					Timestamp: ts,
				},
			})
		}
	}

	if len(items) == 0 || len(items) < r.source.pageSize {
		r.done = true
	}
	r.fetchedAt = ts
	return records, nil
}

// fetchWithRetry performs the HTTP request with exponential backoff on 429/5xx.
func (r *httpReader) fetchWithRetry(ctx context.Context, requestURL string) ([]any, error) {
	var lastErr error
	for attempt := 0; attempt <= r.source.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(r.source.retryBaseMs<<(attempt-1)) * time.Millisecond
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		items, retryable, err := r.doRequest(ctx, requestURL)
		if err == nil {
			return items, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, fmt.Errorf("http fetch failed after %d retries: %w", r.source.maxRetries, lastErr)
}

func (r *httpReader) doRequest(ctx context.Context, requestURL string) ([]any, bool, error) {
	var bodyReader io.Reader
	if r.source.body != "" {
		bodyReader = bytes.NewReader([]byte(r.source.body))
	}
	req, err := http.NewRequestWithContext(ctx, r.source.method, requestURL, bodyReader)
	if err != nil {
		return nil, false, fmt.Errorf("create request: %w", err)
	}
	for k, v := range r.source.headers {
		req.Header.Set(k, v)
	}
	switch r.source.authType {
	case "bearer", "":
		if r.source.authToken != "" {
			req.Header.Set("Authorization", "Bearer "+r.source.authToken)
		}
	case "basic":
		if r.source.authUser != "" {
			req.SetBasicAuth(r.source.authUser, r.source.authPass)
		}
	case "oauth2_client_credentials":
		tok, err := r.source.ensureOAuth2Token(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("oauth2 token: %w", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf(r.source.oauth2HeaderFmt, tok))
	}

	resp, err := r.source.client.Do(req)
	if err != nil {
		// Network errors are retryable.
		return nil, true, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, true, fmt.Errorf("http status %d: %s", resp.StatusCode, string(body))
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, false, fmt.Errorf("http status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
	if err != nil {
		return nil, false, fmt.Errorf("read body: %w", err)
	}

	items, err := extractItems(body, r.source.resultKey)
	if err != nil {
		return nil, false, err
	}
	return items, false, nil
}

// extractItems parses the response body and returns the records slice.
// If resultKey is set, it is used verbatim. Otherwise we attempt the common
// keys data/items/results/records/list.
func extractItems(body []byte, resultKey string) ([]any, error) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}

	if arr, ok := raw.([]any); ok {
		return arr, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, nil
	}
	if resultKey != "" {
		cur := any(m)
		for _, part := range strings.Split(resultKey, ".") {
			cm, ok := cur.(map[string]any)
			if !ok {
				return nil, nil
			}
			cur, ok = cm[part]
			if !ok {
				return nil, nil
			}
		}
		if arr, ok := cur.([]any); ok {
			return arr, nil
		}
		return nil, nil
	}
	for _, k := range []string{"data", "items", "results", "records", "list"} {
		if arr, ok := m[k].([]any); ok {
			return arr, nil
		}
	}
	return nil, nil
}

func applyPageParams(rawURL, pageParam string, page int, sizeParam string, pageSize int) string {
	values, err := url.ParseQuery("")
	if err != nil {
		values = url.Values{}
	}
	if existing, err := url.Parse(rawURL); err == nil {
		values = existing.Query()
		rawURL = existing.Scheme + "://" + existing.Host + existing.Path
	}
	if pageParam != "" {
		values.Set(pageParam, strconv.Itoa(page))
	}
	if sizeParam != "" {
		values.Set(sizeParam, strconv.Itoa(pageSize))
	}
	sep := "?"
	if len(values) == 0 {
		sep = ""
	}
	return rawURL + sep + values.Encode()
}

func (r *httpReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	pos := httpPosition{Page: r.committedPage}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

func (r *httpReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	pos := httpPosition{Page: r.committedPage}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

func (r *httpReader) Close() error { return nil }

type httpPosition struct {
	Page int `json:"page"`
}
