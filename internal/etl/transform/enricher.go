package transform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/state"
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
//	cache_ttl_seconds: 0             // Redis-backed cache TTL (0=off)
//	timeout_seconds: 5
//
// Config (SQL mode):
//
//	mode: "sql"
//	dsn: "user:pass@tcp(host:3306)/db"
//	query: "SELECT name, email FROM users WHERE id = {{.user_id}}"
//	target_field: "user"
//	cache_ttl_seconds: 0
type EnricherTransform struct {
	mode        string
	urlTemplate string
	method      string
	headers     map[string]string
	targetField string
	cacheTTL    time.Duration
	timeout     time.Duration
	onError     string // "pass" (default, silent) | "error" (route failed record to DLQ)

	// Async I/O controls (Phase 1 roadmap "异步 I/O 维表查询增强").
	concurrency int           // max parallel in-flight enrichment calls within a batch
	maxInFlight int           // hard cap on in-flight calls (backpressure bound)
	maxRetries  int           // retry attempts for transient errors (429/5xx/net)
	retryBase   time.Duration // base backoff for retry; exponential 2x up to ~10x

	// SQL mode
	dsn       string
	queryTmpl string
	driver    string
	db        *sql.DB

	// HTTP client
	client *http.Client

	// Cache: lookupKey → JSON value in Redis when cache_ttl_seconds > 0.
	cacheStore state.Store
	pipeline   string
	node       string

	// Concurrency primitives shared across ApplyBatch invocations.
	sem chan struct{}

	// Metrics counters (atomic).
	processed   int64
	hits        int64 // cache hits
	misses      int64 // cache misses that triggered a fetch
	cacheHits   int64
	cacheMisses int64
	timeouts    int64
	retries     int64
	errors      int64
	succeeded   int64
	inFlight    int64
}

func NewEnricherTransform(config map[string]any) (*EnricherTransform, error) {
	t := &EnricherTransform{
		mode:        "http",
		method:      "GET",
		targetField: "enriched",
		timeout:     5 * time.Second,
		headers:     make(map[string]string),
		client:      &http.Client{Timeout: 5 * time.Second},
		onError:     "pass",
		concurrency: 1,
		maxInFlight: 100,
		maxRetries:  0,
		retryBase:   200 * time.Millisecond,
		pipeline:    "default",
		node:        "enricher",
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
	if v, ok := config["cache_ttl_seconds"]; ok {
		if n, ok := toInt(v); ok && n >= 0 {
			t.cacheTTL = time.Duration(n) * time.Second
		}
	}
	if v, ok := config["state_pipeline"].(string); ok && v != "" {
		t.pipeline = v
	}
	if v, ok := config["state_node"].(string); ok && v != "" {
		t.node = v
	}
	if rawHeaders, ok := config["headers"].(map[string]any); ok {
		for k, v := range rawHeaders {
			t.headers[k] = fmt.Sprintf("%v", v)
		}
	}
	// Async I/O controls.
	if v, ok := config["concurrency"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.concurrency = n
		}
	}
	if v, ok := config["max_in_flight"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.maxInFlight = n
		}
	}
	if v, ok := config["max_retries"]; ok {
		if n, ok := toInt(v); ok && n >= 0 {
			t.maxRetries = n
		}
	}
	if v, ok := config["retry_base_ms"]; ok {
		if n, ok := toInt(v); ok && n >= 0 {
			t.retryBase = time.Duration(n) * time.Millisecond
		}
	}
	// Cap concurrency by max_in_flight so the semaphore cannot exceed the
	// backpressure bound.
	if t.concurrency > t.maxInFlight {
		t.concurrency = t.maxInFlight
	}
	if t.concurrency > 1 {
		t.sem = make(chan struct{}, t.concurrency)
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
		// Allow the pool to cover the configured concurrency.
		maxOpen := t.concurrency
		if maxOpen < 5 {
			maxOpen = 5
		}
		db.SetMaxOpenConns(maxOpen)
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

	if t.cacheTTL > 0 {
		store, err := state.NewRedisStoreFromConfig(context.Background(), config)
		if err != nil {
			if t.db != nil {
				_ = t.db.Close()
			}
			return nil, fmt.Errorf("enricher: redis cache requires configured Redis state backend: %w", err)
		}
		t.cacheStore = store
	}

	return t, nil
}

func (t *EnricherTransform) Name() string { return "enricher" }

// TransformMetrics implements core.TransformMetricsProvider.
func (t *EnricherTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Node:      t.node,
		Transform: "enricher",
		Counters: map[string]int64{
			"processed":    atomic.LoadInt64(&t.processed),
			"hits":         atomic.LoadInt64(&t.hits),
			"misses":       atomic.LoadInt64(&t.misses),
			"cache_hits":   atomic.LoadInt64(&t.cacheHits),
			"cache_misses": atomic.LoadInt64(&t.cacheMisses),
			"timeouts":     atomic.LoadInt64(&t.timeouts),
			"retries":      atomic.LoadInt64(&t.retries),
			"errors":       atomic.LoadInt64(&t.errors),
			"succeeded":    atomic.LoadInt64(&t.succeeded),
			"in_flight":    atomic.LoadInt64(&t.inFlight),
		},
	}
}

func (t *EnricherTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		rec.Data = make(map[string]any)
	}
	atomic.AddInt64(&t.processed, 1)

	// Build the lookup key from the template (extract {{.field}} references).
	lookupKey := t.resolveTemplate(t.getTemplate(), rec.Data)
	if lookupKey == "" {
		return rec, nil
	}

	// Check Redis-backed cache when explicitly enabled.
	if t.cacheTTL > 0 && t.cacheStore != nil {
		cached, ok, err := t.cacheStore.Get(ctx, t.pipeline, t.node, lookupKey)
		if err != nil {
			return rec, fmt.Errorf("enricher: cache read failed: %w", err)
		}
		if ok {
			atomic.AddInt64(&t.cacheHits, 1)
			atomic.AddInt64(&t.hits, 1)
			var value any
			if err := json.Unmarshal(cached, &value); err != nil {
				return rec, fmt.Errorf("enricher: decode cached value: %w", err)
			}
			rec.Data[t.targetField] = value
			return rec, nil
		}
		atomic.AddInt64(&t.cacheMisses, 1)
	}
	atomic.AddInt64(&t.misses, 1)

	// Fetch enrichment data with retry/backoff and bounded concurrency.
	result, err := t.fetchWithControls(ctx, rec.Data)
	if err != nil {
		atomic.AddInt64(&t.errors, 1)
		// TF-13: surface the failure instead of silently passing through.
		if t.onError == "error" {
			return rec, core.ClassifiedError{Class: core.ClassifyError(err), Err: fmt.Errorf("enricher: lookup failed: %w", err)}
		}
		return rec, nil // default: non-fatal, record passes through unenriched
	}
	atomic.AddInt64(&t.succeeded, 1)

	// Cache and apply.
	if t.cacheTTL > 0 && t.cacheStore != nil {
		payload, err := json.Marshal(result)
		if err != nil {
			return rec, fmt.Errorf("enricher: encode cached value: %w", err)
		}
		if err := t.cacheStore.Set(ctx, t.pipeline, t.node, lookupKey, payload, t.cacheTTL); err != nil {
			return rec, fmt.Errorf("enricher: cache write failed: %w", err)
		}
	}
	rec.Data[t.targetField] = result
	return rec, nil
}

// ApplyBatch implements core.BatchTransform so the runner can route a whole
// batch through this transform. Records are enriched concurrently up to
// `concurrency`/`max_in_flight`, with per-record failures routed via
// PartialTransformError so the runner sends only failed records to DLQ.
func (t *EnricherTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	out := make([]core.Record, len(recs))
	var failures []core.TransformRecordFailure
	var failedMu sync.Mutex

	collect := func(i int, rec core.Record, err error) {
		if err != nil {
			failedMu.Lock()
			failures = append(failures, core.TransformRecordFailure{Record: rec, Err: err})
			failedMu.Unlock()
			out[i] = rec
			return
		}
		out[i] = rec
	}

	if t.sem == nil || len(recs) <= 1 {
		// Serial path (concurrency <= 1 or trivial batch).
		for i, rec := range recs {
			r, err := t.Apply(ctx, rec)
			collect(i, r, err)
		}
		return out, core.NewPartialTransformError("enricher: batch had failures", failures)
	}

	// Concurrent path: bounded by the semaphore, with a hard in-flight cap.
	var wg sync.WaitGroup
	for i, rec := range recs {
		select {
		case t.sem <- struct{}{}:
		case <-ctx.Done():
			collect(i, rec, ctx.Err())
			continue
		}
		// Backpressure: if in-flight exceeds max_in_flight, block here.
		for atomic.LoadInt64(&t.inFlight) >= int64(t.maxInFlight) {
			select {
			case <-ctx.Done():
				<-t.sem
				collect(i, rec, ctx.Err())
				goto nextLoop
			case <-time.After(10 * time.Millisecond):
			}
		}
		atomic.AddInt64(&t.inFlight, 1)
		wg.Add(1)
		go func(i int, rec core.Record) {
			defer wg.Done()
			defer func() { atomic.AddInt64(&t.inFlight, -1); <-t.sem }()
			r, err := t.Apply(ctx, rec)
			collect(i, r, err)
		}(i, rec)
	nextLoop:
	}
	wg.Wait()
	return out, core.NewPartialTransformError("enricher: batch had failures", failures)
}

// fetchWithControls wraps fetchHTTP/fetchSQL with bounded concurrency
// (enforced by the semaphore when configured) and retry/backoff for
// transient errors.
func (t *EnricherTransform) fetchWithControls(ctx context.Context, data map[string]any) (any, error) {
	var lastErr error
	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&t.retries, 1)
			backoff := t.retryBase * time.Duration(1<<(attempt-1))
			if backoff > 10*t.retryBase {
				backoff = 10 * t.retryBase
			}
			// Honor HTTP 429 Retry-After (seconds) when the previous error
			// carries it, overriding the exponential backoff.
			var httpErr *enricherHTTPError
			if errors.As(lastErr, &httpErr) && httpErr.retryAfter != "" {
				if secs, perr := strconv.Atoi(httpErr.retryAfter); perr == nil && secs > 0 {
					backoff = time.Duration(secs) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		result, err := t.fetchOnce(ctx, data)
		if err == nil {
			return result, nil
		}
		lastErr = err
		// Retry only transient/unknown errors.
		if !core.IsRetryableError(err) {
			return nil, err
		}
	}
	return nil, lastErr
}

func (t *EnricherTransform) fetchOnce(ctx context.Context, data map[string]any) (any, error) {
	// Wrap with an explicit timeout so SQL mode also benefits.
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}
	var result any
	var err error
	switch t.mode {
	case "sql":
		result, err = t.fetchSQL(ctx, data)
	case "http":
		result, err = t.fetchHTTP(ctx, data)
	default:
		return nil, nil
	}
	if err != nil {
		// Track timeouts explicitly.
		if ctxErr := ctx.Err(); ctxErr == context.DeadlineExceeded {
			atomic.AddInt64(&t.timeouts, 1)
		}
		// Classify HTTP errors so retry/metrics routing is explicit.
		err = classifyHTTPError(err)
	}
	return result, err
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
		httpErr := &enricherHTTPError{statusCode: resp.StatusCode, body: string(body)}
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			httpErr.retryAfter = retryAfter
		}
		return nil, httpErr
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

// enricherHTTPError carries the HTTP status code so callers can classify it
// (transient for 429/5xx, auth for 401/403, data for other 4xx) and honor
// Retry-After for 429 responses.
type enricherHTTPError struct {
	statusCode int
	body       string
	retryAfter string
}

func (e *enricherHTTPError) Error() string {
	if e.retryAfter != "" {
		return fmt.Sprintf("enricher: HTTP %d (Retry-After: %s): %s", e.statusCode, e.retryAfter, e.body)
	}
	return fmt.Sprintf("enricher: HTTP %d: %s", e.statusCode, e.body)
}

// classifyHTTPError wraps an error in a ClassifiedError based on its HTTP
// status code so core.IsRetryableError/ClassifyError route it correctly.
func classifyHTTPError(err error) error {
	var httpErr *enricherHTTPError
	if errors.As(err, &httpErr) {
		switch {
		case httpErr.statusCode == 429 || httpErr.statusCode >= 500:
			return core.ClassifiedError{Class: core.ErrorClassTransient, Err: err}
		case httpErr.statusCode == 401 || httpErr.statusCode == 403:
			return core.ClassifiedError{Class: core.ErrorClassAuth, Err: err}
		case httpErr.statusCode >= 400:
			return core.ClassifiedError{Class: core.ErrorClassData, Err: err}
		}
	}
	return err
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
	if t.cacheStore != nil {
		_ = t.cacheStore.Close()
	}
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
//	max_cache_entries: 100000    // optional cache entry cap (0 = unlimited)
//	on_miss: "pass"              // "pass" | "null" | "dlq" | "error"
//	on_refresh_error: "pass"     // "pass" | "error"
type LookupTransform struct {
	dsn         string
	query       string
	joinKey     string
	dimKey      string
	fields      []string
	refreshIv   time.Duration
	lastRefresh time.Time
	maxCache    int
	onMiss      string
	onRefresh   string

	mu    sync.RWMutex
	cache map[string]map[string]any // normalized dimKeyValue → {field: value}
	db    *sql.DB

	store      state.Store
	stateOwner bool
	pipeline   string
	node       string
	stateTTL   time.Duration

	processed          int64
	hits               int64
	misses             int64
	missingKeys        int64
	missNull           int64
	missDLQ            int64
	missError          int64
	refreshSuccesses   int64
	refreshErrors      int64
	refreshErrorDLQ    int64
	restoreSuccesses   int64
	scanErrors         int64
	cacheLimitExceeded int64
}

func NewLookupTransform(config map[string]any) (*LookupTransform, error) {
	t := &LookupTransform{
		joinKey:   "id",
		dimKey:    "id",
		refreshIv: 5 * time.Minute,
		cache:     make(map[string]map[string]any),
		pipeline:  "default",
		node:      "lookup",
		onMiss:    "pass",
		onRefresh: "pass",
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
	if v, ok := config["max_cache_entries"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.maxCache = n
		}
	}
	if v, ok := config["on_miss"].(string); ok && v != "" {
		switch v {
		case "pass", "null", "dlq", "error":
			t.onMiss = v
		default:
			return nil, fmt.Errorf("lookup: on_miss must be pass|null|dlq|error, got %q", v)
		}
	}
	if v, ok := config["on_refresh_error"].(string); ok && v != "" {
		switch v {
		case "pass", "error":
			t.onRefresh = v
		default:
			return nil, fmt.Errorf("lookup: on_refresh_error must be pass|error, got %q", v)
		}
	}
	if v, ok := config["state_pipeline"].(string); ok && v != "" {
		t.pipeline = v
	}
	if v, ok := config["state_node"].(string); ok && v != "" {
		t.node = v
	}
	if v, ok := config["state_ttl_seconds"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.stateTTL = time.Duration(n) * time.Second
		}
	}
	if backend, ok := config["state_backend"].(string); ok && backend != "" {
		switch strings.ToLower(backend) {
		case "redis":
			store, err := state.NewRedisStoreFromConfig(context.Background(), config)
			if err != nil {
				return nil, fmt.Errorf("lookup: open state store: %w", err)
			}
			t.store = store
			t.stateOwner = true
		default:
			return nil, fmt.Errorf("lookup: unsupported state_backend %q", backend)
		}
	}

	if t.dsn == "" || t.query == "" {
		if t.stateOwner && t.store != nil {
			_ = t.store.Close()
		}
		return nil, fmt.Errorf("lookup: dsn and query are required")
	}
	if len(t.fields) == 0 {
		if t.stateOwner && t.store != nil {
			_ = t.store.Close()
		}
		return nil, fmt.Errorf("lookup: at least one field is required")
	}

	// Detect driver from DSN prefix.
	driver := "mysql"
	if strings.HasPrefix(t.dsn, "postgres://") || strings.HasPrefix(t.dsn, "postgresql://") {
		driver = "pgx"
	}
	db, err := sql.Open(driver, t.dsn)
	if err != nil {
		if t.stateOwner && t.store != nil {
			_ = t.store.Close()
		}
		return nil, fmt.Errorf("lookup: open db: %w", err)
	}
	db.SetMaxOpenConns(3)
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		if t.store == nil {
			db.Close()
			return nil, fmt.Errorf("lookup: ping db: %w", err)
		}
		fmt.Printf("[WARN] lookup: ping db failed, will use persisted state if available: %v\n", err)
	}
	t.db = db

	return t, nil
}

func (t *LookupTransform) Name() string { return "lookup" }

// WithStateStore wires a shared state backend into lookup. It is primarily used
// by tests today and provides the same future runner-injection seam as
// deduplicate.
func (t *LookupTransform) WithStateStore(store state.Store, pipeline, node string, ttl time.Duration) *LookupTransform {
	t.store = store
	t.stateOwner = false
	if pipeline != "" {
		t.pipeline = pipeline
	}
	if node != "" {
		t.node = node
	}
	t.stateTTL = ttl
	return t
}

func (t *LookupTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	atomic.AddInt64(&t.processed, 1)
	// Lazy-load cache on first use, or refresh if interval has elapsed.
	t.mu.RLock()
	cacheSize := len(t.cache)
	needsRefresh := cacheSize == 0 || (t.refreshIv > 0 && time.Since(t.lastRefresh) > t.refreshIv)
	t.mu.RUnlock()
	if needsRefresh {
		if err := t.loadCache(ctx); err != nil {
			fmt.Printf("[WARN] lookup: loadCache failed: %v\n", err)
			if t.onRefresh == "error" {
				atomic.AddInt64(&t.refreshErrorDLQ, 1)
				return rec, fmt.Errorf("lookup: refresh failed: %w", err)
			}
			return rec, nil
		}
	}

	if rec.Data == nil {
		return rec, nil
	}

	// Look up the join key value from the record.
	keyVal, ok := rec.Data[t.joinKey]
	if !ok {
		atomic.AddInt64(&t.missingKeys, 1)
		return t.handleLookupMiss(rec, nil, true)
	}

	lookupKey := normalizeLookupKey(keyVal)
	t.mu.RLock()
	dimRow, found := t.cache[lookupKey]
	t.mu.RUnlock()

	if found {
		atomic.AddInt64(&t.hits, 1)
		if rec.Data == nil {
			rec.Data = make(map[string]any)
		}
		for _, f := range t.fields {
			if v, ok := dimRow[f]; ok {
				rec.Data[f] = v
			}
		}
	} else {
		return t.handleLookupMiss(rec, keyVal, false)
	}

	return rec, nil
}

func (t *LookupTransform) handleLookupMiss(rec core.Record, key any, missingKey bool) (core.Record, error) {
	atomic.AddInt64(&t.misses, 1)
	switch t.onMiss {
	case "null":
		atomic.AddInt64(&t.missNull, 1)
		if rec.Data == nil {
			rec.Data = make(map[string]any)
		}
		for _, f := range t.fields {
			rec.Data[f] = nil
		}
		return rec, nil
	case "dlq":
		atomic.AddInt64(&t.missDLQ, 1)
		if missingKey {
			return rec, core.ClassifiedError{
				Class: core.ErrorClassData,
				Err:   fmt.Errorf("lookup: missing join key %q (on_miss=%s)", t.joinKey, t.onMiss),
			}
		}
		return rec, core.ClassifiedError{
			Class: core.ErrorClassData,
			Err:   fmt.Errorf("lookup: no dimension match for key=%v (on_miss=%s)", key, t.onMiss),
		}
	case "error":
		atomic.AddInt64(&t.missError, 1)
		if missingKey {
			return rec, fmt.Errorf("lookup: missing join key %q (on_miss=%s)", t.joinKey, t.onMiss)
		}
		return rec, fmt.Errorf("lookup: no dimension match for key=%v (on_miss=%s)", key, t.onMiss)
	default:
		return rec, nil
	}
}

func (t *LookupTransform) loadCache(ctx context.Context) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() {
		if err != nil {
			atomic.AddInt64(&t.refreshErrors, 1)
		}
	}()

	if len(t.cache) > 0 && t.refreshIv == 0 {
		return nil // already loaded, no refresh
	}
	if t.db == nil {
		restored, err := t.restoreCacheFromStateLocked(ctx)
		if err != nil {
			return err
		}
		if restored {
			atomic.AddInt64(&t.restoreSuccesses, 1)
			return nil
		}
		return fmt.Errorf("lookup: database is not open and no persisted cache is available")
	}

	rows, err := t.db.QueryContext(ctx, t.query)
	if err != nil {
		if restored, restoreErr := t.restoreCacheFromStateLocked(ctx); restoreErr == nil && restored {
			atomic.AddInt64(&t.refreshErrors, 1)
			atomic.AddInt64(&t.restoreSuccesses, 1)
			fmt.Printf("[WARN] lookup: dimension query failed, restored persisted cache: %v\n", err)
			return nil
		}
		return fmt.Errorf("lookup: query dimension table: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	newCache := make(map[string]map[string]any)
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			atomic.AddInt64(&t.scanErrors, 1)
			fmt.Printf("[WARN] lookup: dimension row scan error: %v (row skipped)\n", err)
			continue
		}
		row := make(map[string]any)
		var dimKeyVal any
		for i, col := range cols {
			row[col] = normalizeSQLValue(values[i])
			if col == t.dimKey {
				dimKeyVal = row[col]
			}
		}
		if dimKeyVal != nil {
			key := normalizeLookupKey(dimKeyVal)
			if _, exists := newCache[key]; !exists && t.maxCache > 0 && len(newCache) >= t.maxCache {
				atomic.AddInt64(&t.cacheLimitExceeded, 1)
				return fmt.Errorf("lookup: cache entry limit exceeded: max_cache_entries=%d", t.maxCache)
			}
			newCache[key] = row
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lookup: iterate dimension rows: %w", err)
	}

	t.cache = newCache
	t.lastRefresh = time.Now()
	atomic.AddInt64(&t.refreshSuccesses, 1)
	if err := t.persistCacheToStateLocked(ctx); err != nil {
		fmt.Printf("[WARN] lookup: persist state cache failed: %v\n", err)
	}
	return nil
}

func (t *LookupTransform) persistCacheToStateLocked(ctx context.Context) error {
	if t.store == nil {
		return nil
	}
	for key, row := range t.cache {
		value, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal lookup cache row %q: %w", key, err)
		}
		if err := t.store.Set(ctx, t.pipeline, t.node, key, value, t.stateTTL); err != nil {
			return fmt.Errorf("set lookup state row %q: %w", key, err)
		}
	}
	return nil
}

func (t *LookupTransform) restoreCacheFromStateLocked(ctx context.Context) (bool, error) {
	if t.store == nil {
		return false, nil
	}
	snap, err := t.store.Snapshot(ctx, t.pipeline, t.node)
	if err != nil {
		return false, fmt.Errorf("lookup: snapshot state: %w", err)
	}
	if snap == nil || len(snap.Entries) == 0 {
		return false, nil
	}
	if t.maxCache > 0 && len(snap.Entries) > t.maxCache {
		atomic.AddInt64(&t.cacheLimitExceeded, 1)
		return false, fmt.Errorf("lookup: persisted cache entry limit exceeded: entries=%d max_cache_entries=%d", len(snap.Entries), t.maxCache)
	}
	restored := make(map[string]map[string]any, len(snap.Entries))
	for _, entry := range snap.Entries {
		row := make(map[string]any)
		if err := json.Unmarshal(entry.Value, &row); err != nil {
			return false, fmt.Errorf("lookup: unmarshal state row %q: %w", entry.Key, err)
		}
		restored[entry.Key] = row
	}
	t.cache = restored
	t.lastRefresh = time.Now()
	return true, nil
}

func normalizeLookupKey(v any) string {
	return fmt.Sprint(normalizeSQLValue(v))
}

func normalizeSQLValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func (t *LookupTransform) Close() error {
	var err error
	if t.db != nil {
		err = t.db.Close()
	}
	if t.stateOwner && t.store != nil {
		if stateErr := t.store.Close(); err == nil {
			err = stateErr
		}
	}
	return err
}

func (t *LookupTransform) SnapshotState(ctx context.Context) (string, string, bool, error) {
	if t.store == nil {
		return "", "", false, nil
	}
	snap, err := t.store.Snapshot(ctx, t.pipeline, t.node)
	if err != nil {
		return t.node, "", false, fmt.Errorf("lookup: snapshot state: %w", err)
	}
	if snap == nil || len(snap.Entries) == 0 {
		return t.node, "", false, nil
	}
	return t.node, snap.Version, true, nil
}

func (t *LookupTransform) StateMetrics(ctx context.Context) (core.StateMetrics, bool, error) {
	return stateMetrics(ctx, t.store, t.pipeline, t.node, "lookup")
}

func (t *LookupTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Node:      t.node,
		Transform: t.Name(),
		Counters: map[string]int64{
			"processed":            atomic.LoadInt64(&t.processed),
			"hit":                  atomic.LoadInt64(&t.hits),
			"miss":                 atomic.LoadInt64(&t.misses),
			"missing_key":          atomic.LoadInt64(&t.missingKeys),
			"miss_null":            atomic.LoadInt64(&t.missNull),
			"miss_dlq":             atomic.LoadInt64(&t.missDLQ),
			"miss_error":           atomic.LoadInt64(&t.missError),
			"refresh_success":      atomic.LoadInt64(&t.refreshSuccesses),
			"refresh_error":        atomic.LoadInt64(&t.refreshErrors),
			"refresh_error_dlq":    atomic.LoadInt64(&t.refreshErrorDLQ),
			"restore_success":      atomic.LoadInt64(&t.restoreSuccesses),
			"scan_error":           atomic.LoadInt64(&t.scanErrors),
			"cache_limit_exceeded": atomic.LoadInt64(&t.cacheLimitExceeded),
		},
	}
}
