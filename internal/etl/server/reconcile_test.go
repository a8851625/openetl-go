package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// ── SQL generation ───────────────────────────────────────────────────────────

func TestReconcileSQLGeneration(t *testing.T) {
	cases := []struct {
		dialect      string
		countWant    string
		digestWants  []string
		bucketWants  []string
		tableRefWant string
	}{
		{
			dialect:      reconcileDialectMySQL,
			countWant:    "SELECT COUNT(*) FROM `orders`",
			digestWants:  []string{"SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS(CHAR(1), COALESCE(CAST(`id` AS CHAR), '<NULL>'), COALESCE(CAST(`amount` AS CHAR), '<NULL>')))), 0) FROM `orders`"},
			bucketWants:  []string{"SELECT MOD(`id`, 16), COUNT(*),", "GROUP BY MOD(`id`, 16) ORDER BY 1"},
			tableRefWant: "`orders`",
		},
		{
			dialect:      reconcileDialectDoris,
			countWant:    "SELECT COUNT(*) FROM `orders`",
			digestWants:  []string{"COALESCE(SUM(CRC32(CONCAT_WS(CHAR(1), COALESCE(CAST(`id` AS CHAR), '<NULL>'), COALESCE(CAST(`amount` AS CHAR), '<NULL>')))), 0)"},
			bucketWants:  []string{"GROUP BY MOD(`id`, 16)"},
			tableRefWant: "`orders`",
		},
		{
			dialect:      reconcileDialectPostgres,
			countWant:    `SELECT COUNT(*) FROM "public"."orders"`,
			digestWants:  []string{"COALESCE(SUM(hashtextextended(concat_ws(chr(1), COALESCE(\"id\"::text, '<NULL>'), COALESCE(\"amount\"::text, '<NULL>')), 0)), 0)"},
			bucketWants:  []string{`GROUP BY MOD("id", 16)`},
			tableRefWant: `"public"."orders"`,
		},
		{
			dialect:      reconcileDialectClickHouse,
			countWant:    "SELECT COUNT(*) FROM `orders`",
			digestWants:  []string{"COALESCE(sum(sipHash64(arrayStringConcat([coalesce(toString(`id`), '<NULL>'), coalesce(toString(`amount`), '<NULL>')], char(1)))), 0)"},
			bucketWants:  []string{"SELECT modulo(toInt64(`id`), 16), COUNT(*),", "GROUP BY modulo(toInt64(`id`), 16) ORDER BY 1"},
			tableRefWant: "`orders`",
		},
	}
	cols := []string{"id", "amount"}
	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			ref := reconcileTableRef(tc.dialect, "", "orders")
			if ref != tc.tableRefWant {
				t.Fatalf("table ref = %q, want %q", ref, tc.tableRefWant)
			}
			if got := reconcileCountSQL(tc.dialect, ref); got != tc.countWant {
				t.Fatalf("count SQL =\n%s\nwant\n%s", got, tc.countWant)
			}
			digest := reconcileDigestSQL(tc.dialect, ref, cols)
			for _, want := range tc.digestWants {
				if !strings.Contains(digest, want) {
					t.Fatalf("digest SQL missing %q:\n%s", want, digest)
				}
			}
			bucket := reconcileBucketSQL(tc.dialect, ref, cols, "id", 16)
			for _, want := range tc.bucketWants {
				if !strings.Contains(bucket, want) {
					t.Fatalf("bucket SQL missing %q:\n%s", want, bucket)
				}
			}
			// The bucket statement must project the bucket first so both sides
			// order identically, and must still carry the digest.
			if !strings.HasPrefix(bucket, "SELECT ") || !strings.Contains(bucket, "COUNT(*)") {
				t.Fatalf("bucket SQL shape unexpected:\n%s", bucket)
			}
		})
	}
}

func TestReconcileQuoteIdentEscapes(t *testing.T) {
	if got := reconcileQuoteIdent(reconcileDialectMySQL, "we`ird"); got != "`we``ird`" {
		t.Fatalf("mysql quote = %q", got)
	}
	if got := reconcileQuoteIdent(reconcileDialectPostgres, `we"ird`); got != `"we""ird"` {
		t.Fatalf("postgres quote = %q", got)
	}
	// Postgres keeps an explicit schema when the table carries one.
	if got := reconcileTableRef(reconcileDialectPostgres, "app", "sales.orders"); got != `"sales"."orders"` {
		t.Fatalf("postgres schema-qualified ref = %q", got)
	}
}

// ── config resolution ────────────────────────────────────────────────────────

func TestReconcileSourceTablesAndTargetMapping(t *testing.T) {
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_snapshot_cdc", Config: map[string]any{
			"database": "app",
			"tables":   []any{"orders", "customers"},
		}},
		Sink: pipeline.SinkSpec{Type: "doris", Config: map[string]any{"table_template": "ods_{db}_{table}"}},
	}
	tables, unsupported := reconcileSourceTables(spec)
	if unsupported != "" {
		t.Fatalf("unsupported = %q", unsupported)
	}
	if len(tables) != 2 || tables[0] != "orders" || tables[1] != "customers" {
		t.Fatalf("tables = %v", tables)
	}
	if got := reconcileTargetTable(spec, "orders"); got != "ods_app_orders" {
		t.Fatalf("target table = %q, want ods_app_orders", got)
	}

	static := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "table": "orders"}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "orders_copy"}},
	}
	if got := reconcileTargetTable(static, "orders"); got != "orders_copy" {
		t.Fatalf("static target table = %q", got)
	}
	tables, _ = reconcileSourceTables(static)
	if len(tables) != 1 || tables[0] != "orders" {
		t.Fatalf("single-table source = %v", tables)
	}

	// A query-driven mysql_batch has no table contract.
	querySpec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_batch", Config: map[string]any{"query": "SELECT 1"}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "t"}},
	}
	if _, unsupported := reconcileSourceTables(querySpec); !strings.Contains(unsupported, "query") {
		t.Fatalf("query-mode unsupported = %q", unsupported)
	}

	// Kafka has no SQL surface at all.
	if reconcileDialectForSource("kafka") != "" {
		t.Fatal("kafka must not map to a SQL dialect")
	}
	if reconcileDialectForSink("kafka") != "" || reconcileDialectForSink("file_sink") != "" {
		t.Fatal("non-SQL sinks must not map to a SQL dialect")
	}
}

// ── comparison engine ────────────────────────────────────────────────────────

type fakeReconcileDB struct {
	// tableColumns is returned for the information_schema-style query.
	columns map[string][]columnMetadata
	pk      map[string][]string
	counts  map[string]int64
	digests map[string]string
	// buckets maps "table|key" to (bucket -> (rows,digest)).
	buckets map[string]map[int][2]string
	// rows holds the per-row (key, digest) pairs used by the row-level diff.
	rows map[string][][2]string
	// rowBuckets maps "table|key" to the bucket the row belongs to.
	rowBuckets map[string]int
	// failOn makes the statement containing the marker fail.
	failOn string
}

func (f *fakeReconcileDB) side(role, dialect string) *reconcileSide {
	return &reconcileSide{
		info: reconcileSideInfo{Role: role, Dialect: dialect, Database: "db"},
		queryFn: func(_ context.Context, sqlText string, keys []string) ([][]string, error) {
			if f.failOn != "" && strings.Contains(sqlText, f.failOn) {
				return nil, fmt.Errorf("scripted failure for %q", f.failOn)
			}
			switch {
			case strings.Contains(sqlText, "GROUP BY"):
				// SELECT <bucket>, COUNT(*), <digest> FROM `t` GROUP BY ...
				table := fakeTableOf(sqlText)
				var rows [][]string
				for bucket, v := range f.buckets[table] {
					rows = append(rows, []string{fmt.Sprint(bucket), v[0], v[1]})
				}
				return rows, nil
			case strings.Contains(sqlText, "COUNT(*),"):
				table := fakeTableOf(sqlText)
				return [][]string{{fmt.Sprint(f.counts[table]), f.digests[table]}}, nil
			case strings.HasPrefix(sqlText, "SELECT COUNT(*)"):
				table := fakeTableOf(sqlText)
				return [][]string{{fmt.Sprint(f.counts[table])}}, nil
			case strings.Contains(sqlText, "ORDER BY 1 LIMIT"):
				// Row-level diff sample: SELECT <key>, <digest> FROM <t> [WHERE bucket] ORDER BY 1 LIMIT n
				table := fakeTableOf(sqlText)
				bucket := -1
				if m := fakeBucketFilterPattern.FindStringSubmatch(sqlText); m != nil {
					bucket, _ = strconv.Atoi(m[1])
				}
				limit := 100
				if m := fakeLimitPattern.FindStringSubmatch(sqlText); m != nil {
					limit, _ = strconv.Atoi(m[1])
				}
				var sampled [][]string
				for _, row := range f.rows[table] {
					if bucket >= 0 {
						if b, ok := f.rowBuckets[table+"|"+row[0]]; ok && b != bucket {
							continue
						}
					}
					sampled = append(sampled, []string{row[0], row[1]})
					if len(sampled) >= limit {
						break
					}
				}
				return sampled, nil
			}
			return nil, fmt.Errorf("unexpected statement: %s", sqlText)
		},
	}
}

var fakeBucketFilterPattern = regexp.MustCompile(`WHERE .*= ([0-9]+)`)
var fakeLimitPattern = regexp.MustCompile(`LIMIT ([0-9]+)`)

// fakeFromTablePattern extracts the table name from the FROM clause of a
// generated statement, tolerating both identifier quoting styles.
var fakeFromTablePattern = regexp.MustCompile(`FROM\s+(?:` + "`[^`]+`" + `|"[^"]+")\.(?:` + "`([^`]+)`" + `|"([^"]+)")\s|FROM\s+` + "`([^`]+)`" + `|FROM\s+"([^"]+)"`)

// fakeTableOf extracts the target table name from a generated statement.
func fakeTableOf(sqlText string) string {
	m := fakeFromTablePattern.FindStringSubmatch(sqlText)
	if m == nil {
		return ""
	}
	for _, g := range m[1:] {
		if g != "" {
			return g
		}
	}
	return ""
}

// reconcileColumns is overridden for fake sides by intercepting the metadata
// helpers: fake sides carry their schema in info.Database-keyed maps, so tests
// call the engine with a preloaded server seam.
type fakeReconcileEnv struct {
	source *fakeReconcileDB
	target *fakeReconcileDB
	spec   *pipeline.Spec
}

func (e *fakeReconcileEnv) server() *Server {
	s := &Server{}
	s.reconcileSourceOpener = func(context.Context, *pipeline.Spec) (*reconcileSide, error) {
		return e.source.side("source", reconcileDialectMySQL), nil
	}
	s.reconcileTargetOpener = func(context.Context, *pipeline.Spec) (*reconcileSide, error) {
		return e.target.side("target", reconcileDialectDoris), nil
	}
	s.reconcileColumnsFn = func(_ context.Context, side *reconcileSide, table string) ([]columnMetadata, []string, error) {
		db := e.source
		if side.info.Role == "target" {
			db = e.target
		}
		cols, ok := db.columns[table]
		if !ok {
			return nil, nil, nil
		}
		return cols, db.pk[table], nil
	}
	return s
}

func TestReconcileVerdictsAndBucketLocalisation(t *testing.T) {
	newEnv := func(src, tgt *fakeReconcileDB) (*Server, *pipeline.Spec) {
		spec := &pipeline.Spec{
			Name:   "recon",
			Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "table": "orders"}},
			Sink:   pipeline.SinkSpec{Type: "doris", Config: map[string]any{"table": "orders"}},
		}
		env := &fakeReconcileEnv{source: src, target: tgt, spec: spec}
		return env.server(), spec
	}
	cols := []columnMetadata{{Name: "id"}, {Name: "amount"}}
	pk := map[string][]string{"orders": {"id"}}

	t.Run("match", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		s, spec := newEnv(src, tgt)
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if report.Verdict != reconcileVerdictMatch || report.Summary.Matched != 1 {
			t.Fatalf("report = %+v", report)
		}
		if report.Tables[0].SourceRows != 3 || report.Tables[0].TargetRows != 3 {
			t.Fatalf("rows = %+v", report.Tables[0])
		}
	})

	t.Run("count mismatch localises bucket", func(t *testing.T) {
		src := &fakeReconcileDB{
			columns: map[string][]columnMetadata{"orders": cols},
			pk:      pk,
			counts:  map[string]int64{"orders": 5},
			digests: map[string]string{"orders": "99"},
			buckets: map[string]map[int][2]string{"orders": {0: {"2", "10"}, 1: {"3", "20"}}},
		}
		tgt := &fakeReconcileDB{
			columns: map[string][]columnMetadata{"orders": cols},
			counts:  map[string]int64{"orders": 4},
			digests: map[string]string{"orders": "88"},
			buckets: map[string]map[int][2]string{"orders": {0: {"2", "10"}, 1: {"2", "15"}}},
		}
		s, spec := newEnv(src, tgt)
		// Preload column metadata into the fake sides via the engine's lookup.
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		row := report.Tables[0]
		if row.Verdict != reconcileVerdictCountMismatch {
			t.Fatalf("verdict = %q", row.Verdict)
		}
		if row.RowDelta != 1 {
			t.Fatalf("row delta = %d", row.RowDelta)
		}
		if row.BucketKey != "id" {
			t.Fatalf("bucket key = %q", row.BucketKey)
		}
		if len(row.BucketMismatches) != 1 || row.BucketMismatches[0].Bucket != 1 {
			t.Fatalf("bucket mismatches = %+v", row.BucketMismatches)
		}
		if row.BucketMismatches[0].SourceRows != 3 || row.BucketMismatches[0].TargetRows != 2 {
			t.Fatalf("bucket detail = %+v", row.BucketMismatches[0])
		}
	})

	t.Run("content mismatch", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "43"}}
		s, spec := newEnv(src, tgt)
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if report.Tables[0].Verdict != reconcileVerdictContentDiff {
			t.Fatalf("verdict = %q", report.Tables[0].Verdict)
		}
		if report.Verdict != reconcileVerdictContentDiff {
			t.Fatalf("report verdict = %q", report.Verdict)
		}
	})

	t.Run("deep=false is count only", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "999"}}
		s, spec := newEnv(src, tgt)
		off := false
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{Deep: &off})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		row := report.Tables[0]
		if row.Verdict != reconcileVerdictMatch {
			t.Fatalf("shallow verdict = %q", row.Verdict)
		}
		if row.SourceDigest != "" || row.TargetDigest != "" {
			t.Fatalf("shallow compare must not compute digests: %+v", row)
		}
	})

	t.Run("missing target columns is schema mismatch", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": {{Name: "id"}}}, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}}
		s, spec := newEnv(src, tgt)
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		row := report.Tables[0]
		if row.Verdict != reconcileVerdictSchemaDiff {
			t.Fatalf("verdict = %q", row.Verdict)
		}
		if len(row.MissingTargetColumns) != 1 || row.MissingTargetColumns[0] != "amount" {
			t.Fatalf("missing target columns = %v", row.MissingTargetColumns)
		}
		if report.Verdict != reconcileVerdictSchemaDiff {
			t.Fatalf("report verdict = %q", report.Verdict)
		}
	})

	t.Run("missing target table", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}}
		tgt := &fakeReconcileDB{}
		s, spec := newEnv(src, tgt)
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if report.Tables[0].Verdict != reconcileVerdictMissingTarget {
			t.Fatalf("verdict = %q", report.Tables[0].Verdict)
		}
	})

	t.Run("error is reported per table", func(t *testing.T) {
		src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: pk, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "42"}, failOn: "GROUP BY"}
		tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, counts: map[string]int64{"orders": 3}, digests: map[string]string{"orders": "43"}}
		s, spec := newEnv(src, tgt)
		report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		row := report.Tables[0]
		if row.Verdict != reconcileVerdictContentDiff {
			t.Fatalf("verdict = %q", row.Verdict)
		}
		if !strings.Contains(row.Detail, "桶定位失败") {
			t.Fatalf("bucket failure must be surfaced, got %q", row.Detail)
		}
	})
}

func TestReconcileRejectsUnsupportedShapes(t *testing.T) {
	s := &Server{}
	cases := []struct {
		name string
		spec *pipeline.Spec
		want string
	}{
		{
			name: "kafka source",
			spec: &pipeline.Spec{
				Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"topics": []any{"t"}}},
				Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "t"}},
			},
			want: "SQL query surface",
		},
		{
			name: "file sink",
			spec: &pipeline.Spec{
				Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"table": "t"}},
				Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{}},
			},
			want: "SQL query surface",
		},
		{
			name: "transforms",
			spec: &pipeline.Spec{
				Source:     pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"table": "t"}},
				Sink:       pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "t"}},
				Transforms: []pipeline.TransformSpec{{Type: "rename"}},
			},
			want: "transform",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RunPipelineReconcile(context.Background(), "p1", tc.spec, reconcileRequest{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// ── HTTP surface ─────────────────────────────────────────────────────────────

func TestReconcileHTTPHandler(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := &pipeline.Spec{
		Name:   "recon-http",
		Source: pipeline.SourceSpec{Type: "mysql_snapshot_cdc", Config: map[string]any{"database": "app", "tables": []any{"orders"}}},
		Sink:   pipeline.SinkSpec{Type: "doris", Config: map[string]any{"table": "orders"}},
	}
	s.mu.Lock()
	s.specs["recon-http"] = spec
	s.mu.Unlock()

	// GET documents the supported matrix without touching any database.
	resp, err := http.Get(ts.URL + "/api/v2/pipelines/recon-http/reconcile")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d", resp.StatusCode)
	}
	var caps map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&caps); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if _, ok := caps["sinks"]; !ok {
		t.Fatalf("capabilities missing sinks: %v", caps)
	}

	// POST runs the comparison through the injected openers and returns the
	// report as JSON.
	cols := []columnMetadata{{Name: "id"}}
	src := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, pk: map[string][]string{"orders": {"id"}}, counts: map[string]int64{"orders": 2}, digests: map[string]string{"orders": "7"}}
	tgt := &fakeReconcileDB{columns: map[string][]columnMetadata{"orders": cols}, counts: map[string]int64{"orders": 2}, digests: map[string]string{"orders": "7"}}
	s.reconcileSourceOpener = func(context.Context, *pipeline.Spec) (*reconcileSide, error) {
		return src.side("source", reconcileDialectMySQL), nil
	}
	s.reconcileTargetOpener = func(context.Context, *pipeline.Spec) (*reconcileSide, error) {
		return tgt.side("target", reconcileDialectDoris), nil
	}
	s.reconcileColumnsFn = func(_ context.Context, side *reconcileSide, table string) ([]columnMetadata, []string, error) {
		db := src
		if side.info.Role == "target" {
			db = tgt
		}
		cols, ok := db.columns[table]
		if !ok {
			return nil, nil, nil
		}
		return cols, db.pk[table], nil
	}
	postResp, err := http.Post(ts.URL+"/api/v2/pipelines/recon-http/reconcile", "application/json", strings.NewReader(`{"tables":["orders"]}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d", postResp.StatusCode)
	}
	var report reconcileReport
	if err := json.NewDecoder(postResp.Body).Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Verdict != reconcileVerdictMatch || report.Summary.Tables != 1 {
		t.Fatalf("report = %+v", report)
	}
	if len(report.Limits) == 0 {
		t.Fatal("report must carry its limits")
	}

	// A table outside the pipeline config is rejected, not silently ignored.
	badResp, err := http.Post(ts.URL+"/api/v2/pipelines/recon-http/reconcile", "application/json", strings.NewReader(`{"tables":["nope"]}`))
	if err != nil {
		t.Fatalf("POST bad table: %v", err)
	}
	defer badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad-table status = %d, want 400", badResp.StatusCode)
	}
}

func TestReconcileHTTPMethodNotAllowed(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "table": "orders"}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "orders"}},
	}
	s.mu.Lock()
	s.specs["recon-method"] = spec
	s.mu.Unlock()

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines/recon-method/reconcile", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT status = %d, want 405", resp.StatusCode)
	}
}

func TestReconcileRowLevelDiffSamplesKeys(t *testing.T) {
	cols := []columnMetadata{{Name: "id"}, {Name: "amount"}}
	pk := map[string][]string{"orders": {"id"}}
	src := &fakeReconcileDB{
		columns: map[string][]columnMetadata{"orders": cols},
		pk:      pk,
		counts:  map[string]int64{"orders": 3},
		digests: map[string]string{"orders": "100"},
		buckets: map[string]map[int][2]string{
			"orders": {0: {"2", "a"}, 1: {"1", "c"}},
		},
		rows: map[string][][2]string{
			"orders": {{"1", "a"}, {"2", "b"}, {"3", "c"}},
		},
		rowBuckets: map[string]int{"orders|1": 0, "orders|2": 0, "orders|3": 1},
	}
	tgt := &fakeReconcileDB{
		columns: map[string][]columnMetadata{"orders": cols},
		counts:  map[string]int64{"orders": 3},
		digests: map[string]string{"orders": "200"},
		buckets: map[string]map[int][2]string{
			"orders": {0: {"2", "x"}, 1: {"0", "0"}},
		},
		rows: map[string][][2]string{
			"orders": {{"1", "a"}, {"2", "x"}, {"4", "d"}},
		},
		rowBuckets: map[string]int{"orders|1": 0, "orders|2": 0, "orders|4": 0},
	}
	spec := &pipeline.Spec{
		Name:   "recon-rows",
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "table": "orders"}},
		Sink:   pipeline.SinkSpec{Type: "doris", Config: map[string]any{"table": "orders"}},
	}
	env := &fakeReconcileEnv{source: src, target: tgt, spec: spec}
	s := env.server()

	report, err := s.RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{SampleRows: 10})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(report.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(report.Tables))
	}
	table := report.Tables[0]
	if table.Verdict == reconcileVerdictMatch {
		t.Fatalf("verdict = match, want a mismatch verdict")
	}
	if table.RowDiffScope != reconcileRowDiffScopeBuckets {
		t.Fatalf("row_diff_scope = %q, want %q", table.RowDiffScope, reconcileRowDiffScopeBuckets)
	}
	kinds := map[string]string{}
	for _, diff := range table.RowDiffs {
		kinds[diff.Key] = diff.Kind
	}
	if kinds["3"] != reconcileRowDiffMissingInTarget {
		t.Errorf("key 3 kind = %q, want missing_in_target (all diffs: %v)", kinds["3"], table.RowDiffs)
	}
	if kinds["4"] != reconcileRowDiffExtraInTarget {
		t.Errorf("key 4 kind = %q, want extra_in_target", kinds["4"])
	}
	if kinds["2"] != reconcileRowDiffValueMismatch {
		t.Errorf("key 2 kind = %q, want value_mismatch", kinds["2"])
	}
	if _, ok := kinds["1"]; ok {
		t.Errorf("key 1 matches on both sides and must not be reported: %v", table.RowDiffs)
	}
}

func TestReconcileRowDiffDisabledByDefault(t *testing.T) {
	cols := []columnMetadata{{Name: "id"}, {Name: "amount"}}
	src := &fakeReconcileDB{
		columns: map[string][]columnMetadata{"orders": cols},
		pk:      map[string][]string{"orders": {"id"}},
		counts:  map[string]int64{"orders": 2},
		digests: map[string]string{"orders": "1"},
	}
	tgt := &fakeReconcileDB{
		columns: map[string][]columnMetadata{"orders": cols},
		counts:  map[string]int64{"orders": 3},
		digests: map[string]string{"orders": "2"},
	}
	spec := &pipeline.Spec{
		Name:   "recon-norows",
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "table": "orders"}},
		Sink:   pipeline.SinkSpec{Type: "doris", Config: map[string]any{"table": "orders"}},
	}
	env := &fakeReconcileEnv{source: src, target: tgt, spec: spec}
	report, err := env.server().RunPipelineReconcile(context.Background(), "p1", spec, reconcileRequest{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(report.Tables) != 1 || len(report.Tables[0].RowDiffs) != 0 {
		t.Fatalf("row diffs must stay off unless sample_rows is set: %+v", report.Tables)
	}
}

func TestReconcileRowSQLShapes(t *testing.T) {
	mysqlSQL := reconcileRowSQL(reconcileDialectMySQL, "`db`.`orders`", []string{"id", "amount"}, "id", 16, 3, 25)
	if !strings.Contains(mysqlSQL, "CRC32(CONCAT_WS") || !strings.Contains(mysqlSQL, "MOD(`id`, 16) = 3") || !strings.HasSuffix(mysqlSQL, "ORDER BY 1 LIMIT 25") {
		t.Fatalf("mysql row SQL = %s", mysqlSQL)
	}
	pgSQL := reconcileRowSQL(reconcileDialectPostgres, `"public"."orders"`, []string{"id", "amount"}, "id", 1, -1, 10)
	if !strings.Contains(pgSQL, "hashtextextended(concat_ws") || strings.Contains(pgSQL, "WHERE") {
		t.Fatalf("postgres row SQL without bucketing must not filter: %s", pgSQL)
	}
	chSQL := reconcileRowSQL(reconcileDialectClickHouse, "db.orders", []string{"id", "amount"}, "id", 4, 1, 5)
	if !strings.Contains(chSQL, "sipHash64(arrayStringConcat") || !strings.Contains(chSQL, "modulo(toInt64(`id`), 4) = 1") {
		t.Fatalf("clickhouse row SQL = %s", chSQL)
	}
}

// TestReconcileUnknownPipelineIs404 pins that the reconcile capability endpoint
// answers 404 for an unknown pipeline instead of 200 with a capabilities doc or
// a 400 that blames the request. The action router treats reconcile as a
// standalone action, so the handler itself must reject the missing spec.
func TestReconcileUnknownPipelineIs404(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	_ = s

	resp, err := http.Get(ts.URL + "/api/v2/pipelines/does-not-exist/reconcile")
	if err != nil {
		t.Fatalf("GET capabilities: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET capabilities status = %d, want 404 (body %s)", resp.StatusCode, string(body))
	}

	resp2, err := http.Post(ts.URL+"/api/v2/pipelines/does-not-exist/reconcile", "application/json", strings.NewReader(`{"tables":["x"]}`))
	if err != nil {
		t.Fatalf("POST reconcile: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("POST reconcile status = %d, want 404 (body %s)", resp2.StatusCode, string(body))
	}
}
