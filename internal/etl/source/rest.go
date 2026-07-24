package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("rest_source", func(config map[string]any) (core.Source, error) {
		return NewRestSource(config)
	})
}

// RestSource 是通用 REST API 源：通过配置驱动认证、分页、解析与重试。
type RestSource struct {
	name   string
	url    string
	method string
	headers map[string]string
	body    string
	bodyType string // json | form

	// 认证
	authType       string // none | api_key | basic_auth | bearer | oauth2_client_credentials
	apiKeyHeader   string
	apiKeyQuery    string
	apiKeyValue    string
	username       string
	password       string
	bearerToken    string
	oauth2TokenURL string
	oauth2ClientID string
	oauth2ClientSecret string
	oauth2Scopes   string
	oauth2TokenField string
	oauth2HeaderFmt  string

	// 分页
	pagination  string // offset | cursor | page_token | "" (无)
	pageParam   string
	sizeParam   string
	pageSize    int
	maxPages    int
	totalField  string
	cursorParam string
	cursorField string
	cursorHeader string
	tokenParam  string
	tokenField  string

	// 响应
	resultKey string

	// 重试
	maxRetries  int
	retryBaseMs int

	// 运行时变量（用于 ${var} / ${context.var} 替换）
	variables map[string]string

	// 可选 schema/sample（preflight 使用，本结构仅保留）
	schema any
	sample any

	client *http.Client

	// OAuth2 token 缓存
	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// NewRestSource 从配置构建 RestSource。
func NewRestSource(config map[string]any) (*RestSource, error) {
	s := &RestSource{
		name:            "rest_source",
		method:          "GET",
		bodyType:        "json",
		authType:        "none",
		pageSize:        100,
		maxPages:        0,
		maxRetries:      3,
		retryBaseMs:     500,
		oauth2TokenField: "access_token",
		oauth2HeaderFmt:  "Bearer %s",
		client:          &http.Client{Timeout: 30 * time.Second},
		variables:       map[string]string{},
	}

	if v, ok := config["name"].(string); ok && v != "" {
		s.name = v
	}
	if v, ok := config["url"].(string); ok {
		s.url = v
	}
	if v, ok := config["method"].(string); ok && v != "" {
		s.method = strings.ToUpper(v)
	}
	if v, ok := config["headers"]; ok {
		s.headers = stringMapField(v)
	}
	if v, ok := config["body"].(string); ok {
		s.body = v
	}
	if v, ok := config["body_type"].(string); ok && v != "" {
		s.bodyType = strings.ToLower(v)
	}

	// auth
	if v, ok := config["auth"].(string); ok && v != "" {
		s.authType = strings.ToLower(v)
	} else if v, ok := config["auth_type"].(string); ok && v != "" {
		// 兼容 http source 命名
		s.authType = normalizeRestAuthType(v)
	}
	if v, ok := config["api_key_header"].(string); ok {
		s.apiKeyHeader = v
	}
	if v, ok := config["api_key_query"].(string); ok {
		s.apiKeyQuery = v
	}
	if v, ok := config["api_key_value"].(string); ok {
		s.apiKeyValue = v
	}
	if v, ok := config["username"].(string); ok {
		s.username = v
	} else if v, ok := config["auth_user"].(string); ok {
		s.username = v
	}
	if v, ok := config["password"].(string); ok {
		s.password = v
	} else if v, ok := config["auth_pass"].(string); ok {
		s.password = v
	}
	if v, ok := config["token"].(string); ok {
		s.bearerToken = v
	} else if v, ok := config["auth_token"].(string); ok {
		s.bearerToken = v
	}
	if v, ok := config["token_url"].(string); ok {
		s.oauth2TokenURL = v
	} else if v, ok := config["oauth2_token_url"].(string); ok {
		s.oauth2TokenURL = v
	}
	if v, ok := config["client_id"].(string); ok {
		s.oauth2ClientID = v
	} else if v, ok := config["oauth2_client_id"].(string); ok {
		s.oauth2ClientID = v
	}
	if v, ok := config["client_secret"].(string); ok {
		s.oauth2ClientSecret = v
	} else if v, ok := config["oauth2_client_secret"].(string); ok {
		s.oauth2ClientSecret = v
	}
	if v, ok := config["scopes"].(string); ok {
		s.oauth2Scopes = v
	} else if v, ok := config["oauth2_scopes"].(string); ok {
		s.oauth2Scopes = v
	}
	// header_format 仅用于 oauth2
	if v, ok := config["header_format"].(string); ok && v != "" {
		s.oauth2HeaderFmt = v
	} else if v, ok := config["oauth2_header_format"].(string); ok && v != "" {
		s.oauth2HeaderFmt = v
	}

	// pagination（先读 pagination，再决定 token_field 归属）
	if v, ok := config["pagination"].(string); ok {
		s.pagination = strings.ToLower(v)
	}
	if v, ok := config["page_param"].(string); ok {
		s.pageParam = v
	}
	if v, ok := config["size_param"].(string); ok {
		s.sizeParam = v
	}
	s.pageSize = readInt(config, "page_size", s.pageSize)
	s.maxPages = readInt(config, "max_pages", s.maxPages)
	if v, ok := config["total_field"].(string); ok {
		s.totalField = v
	}
	if v, ok := config["cursor_param"].(string); ok {
		s.cursorParam = v
	}
	if v, ok := config["cursor_field"].(string); ok {
		s.cursorField = v
	}
	if v, ok := config["cursor_header"].(string); ok {
		s.cursorHeader = v
	}
	if v, ok := config["token_param"].(string); ok {
		s.tokenParam = v
	}
	// token_field 在 page_token 分页与 oauth2 间共用键名：
	// - pagination=page_token 时表示响应中的下一页 token 字段
	// - 否则表示 oauth2 响应中的 access token 字段
	// 显式 oauth2_token_field 始终覆盖 oauth2 侧
	if v, ok := config["oauth2_token_field"].(string); ok && v != "" {
		s.oauth2TokenField = v
	}
	if v, ok := config["token_field"].(string); ok && v != "" {
		if s.pagination == "page_token" {
			s.tokenField = v
		} else if _, hasExplicit := config["oauth2_token_field"]; !hasExplicit {
			s.oauth2TokenField = v
		}
	}
	if s.pagination == "page_token" && s.tokenField == "" && s.cursorField != "" {
		s.tokenField = s.cursorField
	}

	if v, ok := config["result_key"].(string); ok {
		s.resultKey = v
	}

	s.maxRetries = readInt(config, "max_retries", s.maxRetries)
	s.retryBaseMs = readInt(config, "retry_base_ms", s.retryBaseMs)

	if v, ok := config["variables"]; ok {
		s.variables = stringMapField(v)
	}
	if v, ok := config["schema"]; ok {
		s.schema = v
	}
	if v, ok := config["sample"]; ok {
		s.sample = v
	}

	if s.url == "" {
		return nil, fmt.Errorf("rest_source requires url")
	}
	switch s.authType {
	case "none", "":
		s.authType = "none"
	case "api_key":
		if s.apiKeyValue == "" {
			return nil, fmt.Errorf("rest_source auth=api_key requires api_key_value")
		}
		if s.apiKeyHeader == "" && s.apiKeyQuery == "" {
			return nil, fmt.Errorf("rest_source auth=api_key requires api_key_header or api_key_query")
		}
	case "basic_auth", "basic":
		s.authType = "basic_auth"
		if s.username == "" {
			return nil, fmt.Errorf("rest_source auth=basic_auth requires username")
		}
	case "bearer":
		if s.bearerToken == "" {
			return nil, fmt.Errorf("rest_source auth=bearer requires token")
		}
	case "oauth2_client_credentials":
		if s.oauth2TokenURL == "" || s.oauth2ClientID == "" || s.oauth2ClientSecret == "" {
			return nil, fmt.Errorf("rest_source auth=oauth2_client_credentials requires token_url, client_id, client_secret")
		}
	default:
		return nil, fmt.Errorf("rest_source unsupported auth %q", s.authType)
	}
	switch s.pagination {
	case "", "none", "offset", "cursor", "page_token":
		if s.pagination == "none" {
			s.pagination = ""
		}
	default:
		return nil, fmt.Errorf("rest_source unsupported pagination %q (want offset|cursor|page_token)", s.pagination)
	}
	if s.pageSize <= 0 {
		return nil, fmt.Errorf("rest_source page_size must be positive")
	}
	if s.maxPages < 0 {
		return nil, fmt.Errorf("rest_source max_pages must be >= 0")
	}
	if s.maxRetries < 0 {
		return nil, fmt.Errorf("rest_source max_retries must be >= 0")
	}
	if s.retryBaseMs < 0 {
		return nil, fmt.Errorf("rest_source retry_base_ms must be >= 0")
	}

	// page_token 默认字段
	if s.pagination == "page_token" {
		if s.tokenParam == "" {
			s.tokenParam = "start_cursor"
		}
		if s.tokenField == "" {
			s.tokenField = "next_cursor"
		}
	}
	if s.pagination == "cursor" {
		if s.cursorParam == "" {
			s.cursorParam = "cursor"
		}
	}
	if s.pagination == "offset" {
		if s.pageParam == "" {
			s.pageParam = "_offset"
		}
		if s.sizeParam == "" {
			s.sizeParam = "_limit"
		}
	}

	return s, nil
}

func normalizeRestAuthType(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "basic":
		return "basic_auth"
	case "bearer", "api_key", "none", "oauth2_client_credentials", "basic_auth":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

func stringMapField(v any) map[string]string {
	out := map[string]string{}
	switch m := v.(type) {
	case map[string]string:
		for k, val := range m {
			out[k] = val
		}
	case map[string]any:
		for k, val := range m {
			if vs, ok := val.(string); ok {
				out[k] = vs
			} else if val != nil {
				out[k] = fmt.Sprint(val)
			}
		}
	}
	return out
}

func (s *RestSource) Name() string { return s.name }

// SetVariables 允许在运行时覆盖/补充模板变量。
func (s *RestSource) SetVariables(vars map[string]string) {
	if s.variables == nil {
		s.variables = map[string]string{}
	}
	for k, v := range vars {
		s.variables[k] = v
	}
}

var restVarPattern = regexp.MustCompile(`\$\{([a-zA-Z0-9_.]+)\}`)

// expandVars 将 ${var} / ${context.var} 替换为 variables 中的值。
func (s *RestSource) expandVars(input string) string {
	if input == "" || !strings.Contains(input, "${") {
		return input
	}
	return restVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		key := restVarPattern.FindStringSubmatch(match)[1]
		if v, ok := s.variables[key]; ok {
			return v
		}
		if strings.HasPrefix(key, "context.") {
			short := strings.TrimPrefix(key, "context.")
			if v, ok := s.variables[short]; ok {
				return v
			}
			if v, ok := s.variables[key]; ok {
				return v
			}
		}
		// 也允许 variables 里写 context.xxx 键，用 xxx 查找
		if v, ok := s.variables["context."+key]; ok {
			return v
		}
		return match
	})
}

func (s *RestSource) fetchOAuth2Token(ctx context.Context) error {
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

func (s *RestSource) ensureOAuth2Token(ctx context.Context) (string, error) {
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

func (s *RestSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	reader := &restReader{
		source: s,
		done:   false,
	}
	if cp != nil && len(cp.Position) > 0 && string(cp.Position) != "null" {
		var pos restPosition
		if err := json.Unmarshal(cp.Position, &pos); err == nil {
			reader.offset = pos.Offset
			reader.pageCount = pos.PageCount
			reader.cursor = pos.Cursor
			reader.pageToken = pos.PageToken
			reader.committed = pos
		}
	}
	return reader, nil
}

// restReader 按分页模式拉取 REST 数据。
type restReader struct {
	source    *RestSource
	buffer    []core.Record
	done      bool
	offset    int
	pageCount int
	cursor    string
	pageToken string
	committed restPosition
}

type restPosition struct {
	Offset    int    `json:"offset"`
	PageCount int    `json:"page_count"`
	Cursor    string `json:"cursor,omitempty"`
	PageToken string `json:"page_token,omitempty"`
}

func (r *restReader) Read(ctx context.Context) (core.Record, error) {
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
		r.committed = restPosition{
			Offset:    r.offset,
			PageCount: r.pageCount,
			Cursor:    r.cursor,
			PageToken: r.pageToken,
		}
	}
	return rec, nil
}

func (r *restReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	if len(r.buffer) == 0 && !r.done {
		recs, err := r.fetchPage(ctx)
		if err != nil {
			return nil, err
		}
		r.buffer = recs
		if len(recs) == 0 {
			r.done = true
		}
	}
	if n <= 0 || n >= len(r.buffer) {
		result := r.buffer
		r.buffer = nil
		r.committed = restPosition{
			Offset:    r.offset,
			PageCount: r.pageCount,
			Cursor:    r.cursor,
			PageToken: r.pageToken,
		}
		return result, nil
	}
	result := r.buffer[:n]
	r.buffer = r.buffer[n:]
	if len(r.buffer) == 0 {
		r.committed = restPosition{
			Offset:    r.offset,
			PageCount: r.pageCount,
			Cursor:    r.cursor,
			PageToken: r.pageToken,
		}
	}
	return result, nil
}

func (r *restReader) fetchPage(ctx context.Context) ([]core.Record, error) {
	if r.source.maxPages > 0 && r.pageCount >= r.source.maxPages {
		r.done = true
		return nil, nil
	}

	items, nextCursor, nextToken, total, err := r.fetchWithRetry(ctx)
	if err != nil {
		return nil, err
	}
	r.pageCount++

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

	// 推进分页状态
	switch r.source.pagination {
	case "offset":
		r.offset += r.source.pageSize
		if len(items) == 0 || len(items) < r.source.pageSize {
			r.done = true
		}
		if total > 0 && r.offset >= total {
			r.done = true
		}
	case "cursor":
		r.cursor = nextCursor
		if nextCursor == "" || len(items) == 0 {
			r.done = true
		}
	case "page_token":
		r.pageToken = nextToken
		if nextToken == "" || len(items) == 0 {
			r.done = true
		}
	default:
		// 无分页：只拉一次
		r.done = true
	}

	return records, nil
}

func (r *restReader) fetchWithRetry(ctx context.Context) (items []any, nextCursor, nextToken string, total int, err error) {
	var lastErr error
	for attempt := 0; attempt <= r.source.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(r.source.retryBaseMs<<(attempt-1)) * time.Millisecond
			// 若上次是 429 且带 Retry-After，优先使用
			if ra, ok := lastErr.(*restHTTPError); ok && ra.retryAfter > 0 {
				delay = ra.retryAfter
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, "", "", 0, ctx.Err()
			}
		}
		items, nextCursor, nextToken, total, retryable, err := r.doRequest(ctx)
		if err == nil {
			return items, nextCursor, nextToken, total, nil
		}
		lastErr = err
		if !retryable {
			return nil, "", "", 0, err
		}
	}
	return nil, "", "", 0, fmt.Errorf("rest_source fetch failed after %d retries: %w", r.source.maxRetries, lastErr)
}

type restHTTPError struct {
	statusCode int
	body       string
	retryAfter time.Duration
}

func (e *restHTTPError) Error() string {
	if e.retryAfter > 0 {
		return fmt.Sprintf("http status %d (Retry-After: %s): %s", e.statusCode, e.retryAfter, e.body)
	}
	return fmt.Sprintf("http status %d: %s", e.statusCode, e.body)
}

func (r *restReader) doRequest(ctx context.Context) (items []any, nextCursor, nextToken string, total int, retryable bool, err error) {
	s := r.source
	requestURL := s.expandVars(s.url)
	method := s.method
	bodyStr := s.expandVars(s.body)

	// 组装分页参数
	requestURL, bodyStr, err = r.applyPagination(requestURL, bodyStr)
	if err != nil {
		return nil, "", "", 0, false, err
	}

	// api_key query 模式
	if s.authType == "api_key" && s.apiKeyQuery != "" {
		requestURL = appendQueryParam(requestURL, s.apiKeyQuery, s.apiKeyValue)
	}

	var bodyReader io.Reader
	if bodyStr != "" {
		bodyReader = bytes.NewReader([]byte(bodyStr))
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return nil, "", "", 0, false, fmt.Errorf("create request: %w", err)
	}

	for k, v := range s.headers {
		req.Header.Set(k, s.expandVars(v))
	}
	if bodyStr != "" {
		switch s.bodyType {
		case "form":
			if req.Header.Get("Content-Type") == "" {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		default:
			if req.Header.Get("Content-Type") == "" {
				req.Header.Set("Content-Type", "application/json")
			}
		}
	}

	if err := s.applyAuth(ctx, req); err != nil {
		return nil, "", "", 0, false, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, "", "", 0, true, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		httpErr := &restHTTPError{statusCode: resp.StatusCode, body: string(body)}
		if resp.StatusCode == http.StatusTooManyRequests {
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, parseErr := strconv.Atoi(strings.TrimSpace(ra)); parseErr == nil && secs > 0 {
					httpErr.retryAfter = time.Duration(secs) * time.Second
				} else if when, parseErr := http.ParseTime(ra); parseErr == nil {
					d := time.Until(when)
					if d > 0 {
						httpErr.retryAfter = d
					}
				}
			}
		}
		return nil, "", "", 0, true, httpErr
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, "", "", 0, false, &restHTTPError{statusCode: resp.StatusCode, body: string(body)}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
	if err != nil {
		return nil, "", "", 0, false, fmt.Errorf("read body: %w", err)
	}

	items, err = extractItems(body, s.resultKey)
	if err != nil {
		return nil, "", "", 0, false, err
	}

	// 解析下一页游标/token/total
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)

	switch s.pagination {
	case "cursor":
		if s.cursorField == "__last_id__" && len(items) > 0 {
			// Stripe 风格：用本页最后一条记录的 id 作为 starting_after
			if last, ok := items[len(items)-1].(map[string]any); ok {
				if id, ok := last["id"].(string); ok {
					nextCursor = id
				}
			}
			// has_more=false 时停止
			if raw != nil {
				if hasMore, ok := raw["has_more"].(bool); ok && !hasMore {
					nextCursor = ""
				}
			}
		} else if s.cursorField != "" && raw != nil {
			nextCursor = lookupStringPath(raw, s.cursorField)
		}
		if nextCursor == "" && s.cursorHeader != "" {
			nextCursor = parseLinkHeaderCursor(resp.Header.Get(s.cursorHeader))
		}
	case "page_token":
		if s.tokenField != "" && raw != nil {
			nextToken = lookupStringPath(raw, s.tokenField)
		}
		// Notion 风格：has_more=false 时清空
		if raw != nil {
			if hasMore, ok := raw["has_more"].(bool); ok && !hasMore {
				nextToken = ""
			}
		}
	case "offset":
		if s.totalField != "" && raw != nil {
			total = lookupIntPath(raw, s.totalField)
		}
	}

	return items, nextCursor, nextToken, total, false, nil
}

func (r *restReader) applyPagination(requestURL, bodyStr string) (string, string, error) {
	s := r.source
	switch s.pagination {
	case "offset":
		requestURL = appendQueryParam(requestURL, s.pageParam, strconv.Itoa(r.offset))
		if s.sizeParam != "" {
			requestURL = appendQueryParam(requestURL, s.sizeParam, strconv.Itoa(s.pageSize))
		}
	case "cursor":
		if r.cursor != "" && s.cursorParam != "" {
			requestURL = appendQueryParam(requestURL, s.cursorParam, r.cursor)
		}
		if s.sizeParam != "" {
			requestURL = appendQueryParam(requestURL, s.sizeParam, strconv.Itoa(s.pageSize))
		}
	case "page_token":
		// page_token 常见于 POST body（Notion）；GET 时走 query
		if r.pageToken != "" {
			if strings.EqualFold(s.method, http.MethodGet) || bodyStr == "" {
				requestURL = appendQueryParam(requestURL, s.tokenParam, r.pageToken)
			} else {
				bodyStr = injectJSONStringField(bodyStr, s.tokenParam, r.pageToken)
			}
		}
		if s.sizeParam != "" && (strings.EqualFold(s.method, http.MethodGet) || bodyStr == "") {
			requestURL = appendQueryParam(requestURL, s.sizeParam, strconv.Itoa(s.pageSize))
		}
	}
	return requestURL, bodyStr, nil
}

func (s *RestSource) applyAuth(ctx context.Context, req *http.Request) error {
	switch s.authType {
	case "none", "":
		return nil
	case "api_key":
		if s.apiKeyHeader != "" {
			req.Header.Set(s.apiKeyHeader, s.apiKeyValue)
		}
		// query 已在 doRequest 中处理
	case "basic_auth":
		req.SetBasicAuth(s.username, s.password)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+s.bearerToken)
	case "oauth2_client_credentials":
		tok, err := s.ensureOAuth2Token(ctx)
		if err != nil {
			return fmt.Errorf("oauth2 token: %w", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf(s.oauth2HeaderFmt, tok))
	}
	return nil
}

func appendQueryParam(rawURL, key, value string) string {
	if key == "" {
		return rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		sep := "?"
		if strings.Contains(rawURL, "?") {
			sep = "&"
		}
		return rawURL + sep + url.QueryEscape(key) + "=" + url.QueryEscape(value)
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}

// injectJSONStringField 将 key/value 注入 JSON 对象 body（简单实现）。
func injectJSONStringField(body, key, value string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		// 非 JSON 时直接追加 query 风格不可行，返回原 body
		return body
	}
	m[key] = value
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return string(out)
}

// parseLinkHeaderCursor 解析 GitHub 风格 Link: <url>; rel="next"
func parseLinkHeaderCursor(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	// 可能有多个关系，用逗号分隔
	parts := strings.Split(linkHeader, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !strings.Contains(part, `rel="next"`) && !strings.Contains(part, `rel=next`) {
			continue
		}
		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start >= 0 && end > start {
			nextURL := part[start+1 : end]
			// 优先取 page / cursor / since 等 query
			if u, err := url.Parse(nextURL); err == nil {
				q := u.Query()
				for _, k := range []string{"cursor", "page", "since", "continuation_token"} {
					if v := q.Get(k); v != "" {
						return v
					}
				}
				// 返回完整 next URL，调用方会作为 cursor_param 值；
				// 当 cursor_param 为空时上层可直接替换 URL，这里返回完整 URL。
				return nextURL
			}
			return nextURL
		}
	}
	return ""
}

func lookupStringPath(m map[string]any, path string) string {
	v := lookupPath(m, path)
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func lookupIntPath(m map[string]any, path string) int {
	v := lookupPath(m, path)
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case json.Number:
		i, _ := t.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(t)
		return i
	default:
		return 0
	}
}

func lookupPath(m map[string]any, path string) any {
	if path == "" || m == nil {
		return nil
	}
	cur := any(m)
	for _, part := range strings.Split(path, ".") {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = cm[part]
		if !ok {
			return nil
		}
	}
	return cur
}

func (r *restReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	data, _ := json.Marshal(r.committed)
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

func (r *restReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	data, _ := json.Marshal(r.committed)
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

func (r *restReader) Close() error { return nil }

// PreflightProbe 执行一次轻量连接验证（供 server preflight 调用）。
// 成功返回 nil；不消费分页状态。
func (s *RestSource) PreflightProbe(ctx context.Context) error {
	requestURL := s.expandVars(s.url)
	if s.authType == "api_key" && s.apiKeyQuery != "" {
		requestURL = appendQueryParam(requestURL, s.apiKeyQuery, s.apiKeyValue)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	for k, v := range s.headers {
		req.Header.Set(k, s.expandVars(v))
	}
	if err := s.applyAuth(ctx, req); err != nil {
		return err
	}
	client := s.client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("http status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
