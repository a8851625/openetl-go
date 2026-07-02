package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("feishu_sheet", func(config map[string]any) (core.Source, error) {
		return NewFeishuSheetSource(config)
	})
}

// FeishuSheetSource pulls rows from a Feishu/Lark spreadsheet using the
// tenant_access_token client-credentials OAuth2 flow. It is a batch-style
// pull source: each Open() fetches the configured sheet_range once and
// emits rows as records. For periodic refresh, schedule the pipeline with
// schedule.type: periodic.
type FeishuSheetSource struct {
	name            string
	appID           string
	appSecret       string
	spreadsheetToken string
	sheetRange      string
	sheetID         string
	baseURL         string
	pollIntervalSec int
	httpClient      *http.Client
	// token management
	tokenMu     sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

// NewFeishuSheetSource builds the source from config.
func NewFeishuSheetSource(config map[string]any) (*FeishuSheetSource, error) {
	s := &FeishuSheetSource{
		name:            "feishu_sheet",
		baseURL:         "https://open.feishu.cn",
		pollIntervalSec: 0,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["app_id"]; ok {
		s.appID, _ = v.(string)
	}
	if v, ok := config["app_secret"]; ok {
		s.appSecret, _ = v.(string)
	}
	if v, ok := config["spreadsheet_token"]; ok {
		s.spreadsheetToken, _ = v.(string)
	}
	if v, ok := config["sheet_range"]; ok {
		s.sheetRange, _ = v.(string)
	}
	if v, ok := config["sheet_id"]; ok {
		s.sheetID, _ = v.(string)
	}
	if v, ok := config["base_url"]; ok {
		if vs, ok := v.(string); ok && vs != "" {
			s.baseURL = vs
		}
	}
	if v, ok := config["poll_interval_sec"]; ok {
		switch p := v.(type) {
		case int:
			s.pollIntervalSec = p
		case float64:
			s.pollIntervalSec = int(p)
		}
	}
	if s.appID == "" || s.appSecret == "" {
		return nil, fmt.Errorf("feishu_sheet source requires app_id and app_secret")
	}
	if s.spreadsheetToken == "" {
		return nil, fmt.Errorf("feishu_sheet source requires spreadsheet_token")
	}
	if s.sheetRange == "" && s.sheetID == "" {
		return nil, fmt.Errorf("feishu_sheet source requires sheet_range or sheet_id")
	}
	return s, nil
}

func (s *FeishuSheetSource) Name() string { return s.name }

// fetchToken implements the client_credentials flow:
// POST {base_url}/open-apis/auth/v3/tenant_access_token/internal
// Body: {"app_id": ..., "app_secret": ...}
func (s *FeishuSheetSource) fetchToken(ctx context.Context) error {
	body := fmt.Sprintf(`{"app_id":%q,"app_secret":%q}`, s.appID, s.appSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/open-apis/auth/v3/tenant_access_token/internal", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("feishu token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("feishu token fetch: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("feishu token fetch: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	var parsed struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return fmt.Errorf("feishu token decode: %w", err)
	}
	if parsed.Code != 0 {
		return fmt.Errorf("feishu token error code=%d msg=%s", parsed.Code, parsed.Msg)
	}
	s.tokenMu.Lock()
	s.accessToken = parsed.TenantAccessToken
	// Refresh 60 seconds before expiry to avoid using a stale token.
	s.tokenExpiry = time.Now().Add(time.Duration(parsed.Expire)*time.Second - 60*time.Second)
	s.tokenMu.Unlock()
	return nil
}

// ensureToken returns a valid access token, refreshing if necessary.
func (s *FeishuSheetSource) ensureToken(ctx context.Context) (string, error) {
	s.tokenMu.Lock()
	if s.accessToken != "" && time.Now().Before(s.tokenExpiry) {
		tok := s.accessToken
		s.tokenMu.Unlock()
		return tok, nil
	}
	s.tokenMu.Unlock()
	if err := s.fetchToken(ctx); err != nil {
		return "", err
	}
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	return s.accessToken, nil
}

// fetchSheet fetches the spreadsheet values for the configured range.
// API: GET {base_url}/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/values/{range}
func (s *FeishuSheetSource) fetchSheet(ctx context.Context) ([][]any, error) {
	tok, err := s.ensureToken(ctx)
	if err != nil {
		return nil, err
	}
	rangePath := s.sheetRange
	if rangePath == "" {
		rangePath = s.sheetID
	}
	url := fmt.Sprintf("%s/open-apis/sheets/v3/spreadsheets/%s/values/%s", s.baseURL, s.spreadsheetToken, rangePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("feishu sheet request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("feishu sheet fetch: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("feishu sheet fetch: rate limited (429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("feishu sheet fetch: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	var parsed struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			ValueRange struct {
				Values [][]any `json:"values"`
			} `json:"value_range"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("feishu sheet decode: %w", err)
	}
	if parsed.Code != 0 {
		return nil, fmt.Errorf("feishu sheet error code=%d msg=%s", parsed.Code, parsed.Msg)
	}
	return parsed.Data.ValueRange.Values, nil
}

func (s *FeishuSheetSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	rows, err := s.fetchSheet(ctx)
	if err != nil {
		return nil, err
	}
	return &feishuSheetReader{source: s, rows: rows, offset: 0}, nil
}

type feishuSheetReader struct {
	source *FeishuSheetSource
	rows   [][]any
	offset int
	header []string
}

func (r *feishuSheetReader) Read(ctx context.Context) (core.Record, error) {
	if r.offset >= len(r.rows) {
		return core.Record{}, io.EOF
	}
	row := r.rows[r.offset]
	r.offset++
	data := make(map[string]any)
	// First row is treated as header; subsequent rows use header keys.
	if r.offset == 1 && len(row) > 0 {
		// Detect header row (all string values).
		isHeader := true
		for _, v := range row {
			if _, ok := v.(string); !ok {
				isHeader = false
				break
			}
		}
		if isHeader {
			r.header = make([]string, len(row))
			for i, v := range row {
				r.header[i] = fmt.Sprint(v)
			}
			return r.Read(ctx)
		}
	}
	if len(r.header) > 0 {
		for i, v := range row {
			if i < len(r.header) {
				data[r.header[i]] = v
			} else {
				data[fmt.Sprintf("col_%d", i)] = v
			}
		}
	} else {
		for i, v := range row {
			data[fmt.Sprintf("col_%d", i)] = v
		}
	}
	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			Source:    r.source.name,
			Timestamp: time.Now(),
			Offset:    int64(r.offset),
		},
	}, nil
}

func (r *feishuSheetReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var records []core.Record
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, rec)
	}
	if len(records) == 0 {
		return nil, io.EOF
	}
	return records, nil
}

func (r *feishuSheetReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}

func (r *feishuSheetReader) Close() error { return nil }
