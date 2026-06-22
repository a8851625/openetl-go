package transform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("enricher", func(config map[string]any) (core.Transform, error) {
		return NewEnricherTransform(config)
	})
	registry.RegisterTransform("lookup", func(config map[string]any) (core.Transform, error) {
		return NewLookupTransform(config)
	})
}

// ════════════════════════════════════════════════════════════════════
// Enricher — HTTP/DB field enrichment
// ════════════════════════════════════════════════════════════════════

// EnricherTransform calls an external HTTP API or database query to enrich
// each record with additional fields. Results are cached to avoid duplicate
// calls for the same key.
//
// Config (HTTP mode):
//
//	mode: "http"
//	url: "https://api.example.com/user/{{.user_id}}"
//	method: "GET"
//	headers: { Authorization: "Bearer xxx" }
//	target_field: "user_info"       // JSON response stored under this field
//	cache_ttl_seconds: 300           // cache results for 5 minutes
//	timeout_seconds: 5
//
// Config (SQL mode):
//
//	mode: "sql"
//	dsn: "user:pass@tcp(host:3306)/db"
//	query: "SELECT name, email FROM users WHERE id = {{.user_id}}"
//	target_field: "user"
//	cache_ttl_seconds: 600
type EnricherTransform struct {
	mode        string
	urlTemplate string
	method      string
	headers     map[string]string
	targetField string
	cacheTTL    time.Duration
	timeout     time.Duration
	onError     string // "pass" (default, silent) | "error" (route failed record to DLQ)

	// SQL mode
	dsn       string
	queryTmpl string
	driver    string
	db        *sql.DB

	// HTTP client
	client *http.Client

	// Cache: lookupKey → {value, expiry}
	cache  sync.Map
	stopCh chan struct{} // stops the background eviction loop (TF-13)
}

type enricherCacheEntry struct {
	value  any
	expiry time.Time
}

func NewEnricherTransform(config map[string]any) (*EnricherTransform, error) {
	t := &EnricherTransform{
		mode:        "http",
		method:      "GET",
		targetField: "enriched",
		cacheTTL:    5 * time.Minute,
		timeout:     5 * time.Second,
		headers:     make(map[string]string),
		client:      &http.Client{Timeout: 5 * time.Second},
		onError:     "pass",
		stopCh:      make(chan struct{}),
	}

	if v, ok := config["mode"].(string); ok {
		t.mode = v
	}
	if v, ok := config["url"].(string); ok {
		t.urlTemplate = v
	}
	if v, ok := config["method"].(string); ok {
		t.method = v
	}
	if v, ok := config["target_field"].(string); ok {
		t.targetField = v
	}
	if v, ok := config["timeout_seconds"].(int); ok && v > 0 {
		t.timeout = time.Duration(v) * time.Second
		t.client.Timeout = t.timeout
	}
	if v, ok := config["cache_ttl_seconds"].(int); ok && v > 0 {
		t.cacheTTL = time.Duration(v) * time.Second
	}
	if rawHeaders, ok := config["headers"].(map[string]any); ok {
		for k, v := range rawHeaders {
			t.headers[k] = fmt.Sprintf("%v", v)
		}
	}
	// SQL mode
	if v, ok := config["dsn"].(string); ok {
		t.dsn = v
	}
	if v, ok := config["query"].(string); ok {
		t.queryTmpl = v
	}

	if t.mode == "sql" && t.dsn != "" {
		driver := "mysql"
		if strings.HasPrefix(t.dsn, "postgres://") || strings.HasPrefix(t.dsn, "postgresql://") {
			driver = "pgx"
		}
		t.driver = driver
		db, err := sql.Open(driver, t.dsn)
		if err != nil {
			return nil, fmt.Errorf("enricher: open db: %w", err)
		}
		db.SetMaxOpenConns(5)
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, fmt.Errorf("enricher: ping db: %w", err)
		}
		t.db = db
	}

	// on_error: "pass" (default — enrichment failure is non-fatal, record
	// passes through unenriched) | "error" (return the error so the pipeline
	// routes the record to DLQ — surfaces flaky enrichment endpoints). (TF-13)
	t.onError = "pass"
	if v, ok := config["on_error"].(string); ok {
		switch v {
		case "pass", "error":
			t.onError = v
		}
	}

	// Background cache eviction (TF-13): without this, expired entries lingered
	// in the sync.Map forever, unbounded on high-cardinality keys.
	go t.evictLoop()

	return t, nil
}

// evictLoop periodically deletes expired cache entries so the sync.Map doesn't
// grow without bound on high-cardinality lookup keys. Stops on Close.
func (t *EnricherTransform) evictLoop() {
	interval := t.cacheTTL
	if interval <= 0 || interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			t.cache.Range(func(k, v any) bool {
				if entry, ok := v.(enricherCacheEntry); ok && !now.Before(entry.expiry) {
					t.cache.Delete(k)
				}
				return true
			})
		case <-t.stopCh:
			return
		}
	}
}

func (t *EnricherTransform) Name() string { return "enricher" }

func (t *EnricherTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		rec.Data = make(map[string]any)
	}

	// Build the lookup key from the template (extract {{.field}} references).
	lookupKey := t.resolveTemplate(t.getTemplate(), rec.Data)
	if lookupKey == "" {
		return rec, nil
	}

	// Check cache.
	if cached, ok := t.cache.Load(lookupKey); ok {
		entry := cached.(enricherCacheEntry)
		if time.Now().Before(entry.expiry) {
			rec.Data[t.targetField] = entry.value
			return rec, nil
		}
		// Lazy eviction of the expired entry (in addition to the background sweep).
		t.cache.Delete(lookupKey)
	}

	// Fetch enrichment data.
	var result any
	var err error
	switch t.mode {
	case "sql":
		result, err = t.fetchSQL(ctx, rec.Data)
	case "http":
		result, err = t.fetchHTTP(ctx, rec.Data)
	default:
		return rec, nil
	}
	if err != nil {
		// TF-13: surface the failure instead of silently passing through.
		if t.onError == "error" {
			return rec, fmt.Errorf("enricher: lookup failed: %w", err)
		}
		return rec, nil // default: non-fatal, record passes through unenriched
	}

	// Cache and apply.
	t.cache.Store(lookupKey, enricherCacheEntry{
		value:  result,
		expiry: time.Now().Add(t.cacheTTL),
	})
	rec.Data[t.targetField] = result
	return rec, nil
}

func (t *EnricherTransform) getTemplate() string {
	if t.mode == "sql" {
		return t.queryTmpl
	}
	return t.urlTemplate
}

func (t *EnricherTransform) resolveTemplate(tmpl string, data map[string]any) string {
	result := tmpl
	for k, v := range data {
		placeholder := fmt.Sprintf("{{.%s}}", k)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", v))
	}
	return result
}

// resolveSQLQuery extracts {{.field}} placeholders from the query template,
// replaces them with SQL placeholders (?, $1, etc.), and returns the
// parameterized query along with the argument values in order.
// This prevents SQL injection by ensuring record data never enters the
// query string directly.
func (t *EnricherTransform) resolveSQLQuery(tmpl string, data map[string]any) (string, []any) {
	// Find all {{.field}} references in order of appearance.
	var fields []string
	result := tmpl
	for k := range data {
		placeholder := fmt.Sprintf("{{.%s}}", k)
		if strings.Contains(result, placeholder) {
			fields = append(fields, k)
		}
	}
	// Sort fields by their first position in the template to maintain order.
	type fieldPos struct {
		name string
		pos  int
	}
	var positions []fieldPos
	for _, f := range fields {
		placeholder := fmt.Sprintf("{{.%s}}", f)
		idx := strings.Index(tmpl, placeholder)
		positions = append(positions, fieldPos{name: f, pos: idx})
	}
	// Sort by position.
	for i := 0; i < len(positions); i++ {
		for j := i + 1; j < len(positions); j++ {
			if positions[j].pos < positions[i].pos {
				positions[i], positions[j] = positions[j], positions[i]
			}
		}
	}

	// Replace placeholders in position order with SQL parameter markers.
	var args []any
	for _, fp := range positions {
		placeholder := fmt.Sprintf("{{.%s}}", fp.name)
		if t.driver == "pgx" {
			result = strings.Replace(result, placeholder, fmt.Sprintf("$%d", len(args)+1), 1)
		} else {
			result = strings.Replace(result, placeholder, "?", 1)
		}
		args = append(args, data[fp.name])
	}
	return result, args
}

func (t *EnricherTransform) fetchHTTP(ctx context.Context, data map[string]any) (any, error) {
	url := t.resolveTemplate(t.urlTemplate, data)
	req, err := http.NewRequestWithContext(ctx, t.method, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, fmt.Errorf("enricher: HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return nil, err
	}

	var result any
	if err := json.Unmarshal(body, &result); err != nil {
		return string(body), nil // store as string if not JSON
	}
	return result, nil
}

func (t *EnricherTransform) fetchSQL(ctx context.Context, data map[string]any) (any, error) {
	if t.db == nil {
		return nil, fmt.Errorf("enricher: db not initialized")
	}
	query, args := t.resolveSQLQuery(t.queryTmpl, data)
	rows, err := t.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any)
		for i, col := range cols {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	if len(results) == 0 {
		return nil, nil
	}
	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}

func (t *EnricherTransform) Close() error {
	close(t.stopCh) // stop the background cache-eviction loop
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════
// Lookup — In-memory dimension table join (stream-table join)
// ════════════════════════════════════════════════════════════════════

// LookupTransform maintains an in-memory cache of a dimension table and
// joins each record against it, adding the dimension fields.
//
// This is the lightweight equivalent of Flink's "stream-table join":
//   - On init (first record), it loads the full dimension table into memory.
//   - CDC events for the dimension table update the cache in real-time.
//   - Each record is enriched with matching dimension fields.
//
// Config:
//
//	mode: "mysql" | "postgres"
//	dsn: "user:pass@tcp(host:3306)/dimdb"
//	query: "SELECT id, name, region FROM dim_users"
//	join_key: "user_id"          // field in the record to match on
//	dim_key: "id"                // column in the dimension table
//	fields: [name, region]       // which dimension columns to copy
//	refresh_interval_sec: 300    // full refresh interval (0 = no auto-refresh)
type LookupTransform struct {
	dsn         string
	query       string
	joinKey     string
	dimKey      string
	fields      []string
	refreshIv   time.Duration
	lastRefresh time.Time

	mu    sync.RWMutex
	cache map[any]map[string]any // dimKeyValue → {field: value}
	db    *sql.DB
}

func NewLookupTransform(config map[string]any) (*LookupTransform, error) {
	t := &LookupTransform{
		joinKey:   "id",
		dimKey:    "id",
		refreshIv: 5 * time.Minute,
		cache:     make(map[any]map[string]any),
	}

	if v, ok := config["dsn"].(string); ok {
		t.dsn = v
	}
	if v, ok := config["query"].(string); ok {
		t.query = v
	}
	if v, ok := config["join_key"].(string); ok {
		t.joinKey = v
	}
	if v, ok := config["dim_key"].(string); ok {
		t.dimKey = v
	}
	if rawFields, ok := config["fields"].([]any); ok {
		for _, f := range rawFields {
			if fs, ok := f.(string); ok {
				t.fields = append(t.fields, fs)
			}
		}
	}
	if v, ok := config["refresh_interval_sec"].(int); ok && v >= 0 {
		if v == 0 {
			t.refreshIv = 0 // no auto-refresh
		} else {
			t.refreshIv = time.Duration(v) * time.Second
		}
	}

	if t.dsn == "" || t.query == "" {
		return nil, fmt.Errorf("lookup: dsn and query are required")
	}
	if len(t.fields) == 0 {
		return nil, fmt.Errorf("lookup: at least one field is required")
	}

	// Detect driver from DSN prefix.
	driver := "mysql"
	if strings.HasPrefix(t.dsn, "postgres://") || strings.HasPrefix(t.dsn, "postgresql://") {
		driver = "pgx"
	}
	db, err := sql.Open(driver, t.dsn)
	if err != nil {
		return nil, fmt.Errorf("lookup: open db: %w", err)
	}
	db.SetMaxOpenConns(3)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("lookup: ping db: %w", err)
	}
	t.db = db

	return t, nil
}

func (t *LookupTransform) Name() string { return "lookup" }

func (t *LookupTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	// Lazy-load cache on first use, or refresh if interval has elapsed.
	t.mu.RLock()
	cacheSize := len(t.cache)
	needsRefresh := cacheSize == 0 || (t.refreshIv > 0 && time.Since(t.lastRefresh) > t.refreshIv)
	t.mu.RUnlock()
	if needsRefresh {
		if err := t.loadCache(ctx); err != nil {
			fmt.Printf("[WARN] lookup: loadCache failed: %v\n", err)
			return rec, nil
		}
	}

	if rec.Data == nil {
		return rec, nil
	}

	// Look up the join key value from the record.
	keyVal, ok := rec.Data[t.joinKey]
	if !ok {
		return rec, nil
	}

	t.mu.RLock()
	dimRow, found := t.cache[keyVal]
	t.mu.RUnlock()

	if found {
		if rec.Data == nil {
			rec.Data = make(map[string]any)
		}
		for _, f := range t.fields {
			if v, ok := dimRow[f]; ok {
				rec.Data[f] = v
			}
		}
	}

	return rec, nil
}

func (t *LookupTransform) loadCache(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.cache) > 0 && t.refreshIv == 0 {
		return nil // already loaded, no refresh
	}

	rows, err := t.db.QueryContext(ctx, t.query)
	if err != nil {
		return fmt.Errorf("lookup: query dimension table: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	newCache := make(map[any]map[string]any)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Printf("[WARN] lookup: dimension row scan error: %v (row skipped)\n", err)
			continue
		}
		row := make(map[string]any)
		var dimKeyVal any
		for i, col := range cols {
			row[col] = values[i]
			if col == t.dimKey {
				dimKeyVal = values[i]
			}
		}
		if dimKeyVal != nil {
			newCache[dimKeyVal] = row
		}
	}

	t.cache = newCache
	t.lastRefresh = time.Now()
	return nil
}

func (t *LookupTransform) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}
