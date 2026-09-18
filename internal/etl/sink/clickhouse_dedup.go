package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// CH-C1 (IT-5/T5.1): ClickHouse write-ack and dedup contract.
//
// The sink derives a deterministic dedup token for every successful Write
// batch: pipeline key + source position + batch sequence + record count.
// The same batch replayed after a crash (sink acknowledged, checkpoint not
// yet committed) produces the same token, so ClickHouse's insert_dedup_token
// can drop the duplicate block server-side when supported; otherwise the
// existing ReplacingMergeTree source-order version contract absorbs the
// replay. Either way the terminal state is identical — that equivalence is
// what the crash-window e2e proves.

// dedupTokenDerivationCost guards against pathological source positions.
const maxDedupPositionLen = 4096

// sinkDedupState carries the last successful write's commit metadata until
// the runner collects it via SinkCommitMetadata (after Write returns, before
// checkpoint save). A failed Write resets it: partial batches must never
// surface a stale token as the boundary of a committed batch.
type sinkDedupState struct {
	token       string
	protocol    string
	batchSeq    uint64
	recordCount int
	ackedAt     time.Time
	// tableBoundary records per-table row counts within the batch for
	// multi-table fan-out auditing.
	tableRows map[string]int
}

// positionString renders the strongest available per-record source position.
// Order of preference: binlog file:pos (MySQL CDC/snapshot+CDC), GTID,
// Kafka partition@offset, PostgreSQL LSN, batch cursor. Metadata fields are
// documented contracts; guessing is not allowed — an empty position is an
// explicit "no position" marker, never a fabricated one.
func positionString(m core.Metadata) string {
	var b strings.Builder
	if m.BinlogFile != "" {
		b.WriteString(m.BinlogFile)
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(uint64(m.BinlogPos), 10))
		return b.String()
	}
	if m.Gtid != "" {
		return "gtid:" + m.Gtid
	}
	if m.Partition != 0 || m.Offset != 0 {
		b.WriteString("kafka:")
		b.WriteString(strconv.FormatInt(int64(m.Partition), 10))
		b.WriteByte('@')
		b.WriteString(strconv.FormatInt(m.Offset, 10))
		return b.String()
	}
	if m.LSN != "" {
		return "lsn:" + m.LSN
	}
	if m.Cursor != "" {
		return "cursor:" + m.Cursor
	}
	return ""
}

// deriveDedupToken produces a stable token for a logical write batch. The
// same (pipeline, first position, last position, batch sequence, record
// count) tuple always yields the same hex string; no wall-clock or random
// component participates, so a replay after crash re-derives it exactly.
func deriveDedupToken(pipelineKey string, firstPos, lastPos string, batchSeq uint64, recordCount int) string {
	h := sha256.New()
	writeLenPrefixed(h, pipelineKey)
	writeLenPrefixed(h, firstPos)
	writeLenPrefixed(h, lastPos)
	var seq [8]byte
	for i := 0; i < 8; i++ {
		seq[i] = byte(batchSeq >> (8 * i))
	}
	h.Write(seq[:])
	var cnt [8]byte
	n := uint64(recordCount)
	for i := 0; i < 8; i++ {
		cnt[i] = byte(n >> (8 * i))
	}
	h.Write(cnt[:])
	return hex.EncodeToString(h.Sum(nil))
}

// writeLenPrefixed writes a length-prefixed component so embedded NUL bytes
// in one field cannot alias into the separator position of another.
func writeLenPrefixed(h interface{ Write([]byte) (int, error) }, s string) {
	if len(s) > maxDedupPositionLen {
		s = s[:maxDedupPositionLen]
	}
	var ln [4]byte
	n := uint32(len(s))
	for i := 0; i < 4; i++ {
		ln[i] = byte(n >> (8 * i))
	}
	_, _ = h.Write(ln[:])
	_, _ = h.Write([]byte(s))
}

// batchPosition extracts the boundary positions of the records that were
// actually acknowledged by the sink in this Write call. Returns
// ("", "", false) when no positioned records exist (e.g. DDL-only batch).
func batchPosition(records []core.Record) (first, last string, ok bool) {
	for _, rec := range records {
		if rec.Operation == core.OpDDL {
			continue
		}
		pos := positionString(rec.Metadata)
		if pos == "" {
			continue
		}
		if !ok {
			first = pos
			ok = true
		}
		last = pos
	}
	return first, last, ok
}

// SetPipelineKey binds the dedup-token namespace to the pipeline identity
// (core.PipelineKeySetter, injected by the runner after BuildSink).
func (s *ClickHouseSink) SetPipelineKey(key string) {
	if key == "" {
		key = "clickhouse-sink"
	}
	s.dedupPipelineKey.Store(key)
}

// pendingDedupToken returns the token for the in-flight batch (derived at
// beginDedupBatch time) so write paths can attach it to the INSERT.
func (s *ClickHouseSink) pendingDedupToken() string {
	key, _ := s.dedupPipelineKey.Load().(string)
	if key == "" {
		key = "clickhouse-sink"
	}
	return deriveDedupToken(key, s.dedupFirst, s.dedupLast, s.dedupBatchSeq, int(s.dedupBatchRows.Load()))
}

// clickhouseWithSettings derives a context carrying per-query settings for
// the native driver (PrepareBatch reads queryOptions from ctx).
func clickhouseWithSettings(ctx context.Context, settings map[string]any) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(settings))
}

// insert_dedup_token setting. ClickHouse exposes it since 22.10-ish and it
// became non-experimental in 23.x; rather than trusting a version string we
// probe the merge tree settings profile and fall back to "record-only"
// tokens (logged as a warning) when unsupported.
func (s *ClickHouseSink) detectInsertDedupSupport(ctx context.Context) {
	if s.insertDedupProbeDone.Swap(true) {
		return
	}
	if s.conn == nil && s.httpConn == nil {
		return
	}
	// The setting is visible in system.settings on servers that accept it.
	var val string
	err := s.queryRowContext(ctx, "SELECT value FROM system.settings WHERE name = 'insert_dedup_token'").Scan(&val)
	if err != nil {
		// Setting unknown to this server: tokens are still recorded in
		// checkpoint metadata (audit trail) but not pushed as a setting.
		s.insertDedupSupported.Store(false)
		return
	}
	s.insertDedupSupported.Store(true)
}

// beginDedupBatch stamps the batch sequence used for token derivation. Called
// once per Write before any table sub-batch goes out.
func (s *ClickHouseSink) beginDedupBatch(records []core.Record) {
	s.dedupBatchSeq = s.writeBatches.Add(1)
	s.dedupFirst, s.dedupLast, s.dedupPositioned = batchPosition(records)
	rows := 0
	for _, rec := range records {
		if rec.Operation != core.OpDDL {
			rows++
		}
	}
	s.dedupBatchRows.Store(int64(rows))
}

// finalizeDedupBatch records the commit metadata after every table sub-batch
// in the Write call succeeded. Any later failure path must call
// invalidateDedupBatch instead.
func (s *ClickHouseSink) finalizeDedupBatch(pipelineKey string, records []core.Record, tableRows map[string]int) {
	count := 0
	for _, n := range tableRows {
		count += n
	}
	if count == 0 {
		count = len(records)
	}
	s.dedupState.Store(&sinkDedupState{
		token:       deriveDedupToken(pipelineKey, s.dedupFirst, s.dedupLast, s.dedupBatchSeq, count),
		protocol:    s.protocol,
		batchSeq:    s.dedupBatchSeq,
		recordCount: count,
		ackedAt:     time.Now().UTC(),
		tableRows:   tableRows,
	})
}

// invalidateDedupBatch clears the pending state after a failed Write. The
// last successful batch's metadata must remain retrievable only when the
// whole Write succeeded; a mid-batch failure leaves no verifiable boundary.
func (s *ClickHouseSink) invalidateDedupBatch() {
	s.dedupState.Store(nil)
}

// SinkCommitMetadata implements core.SinkCommitMetadataProvider: the runner
// persists this alongside the source position in the checkpoint envelope.
// The error contract is intentionally strict: an unacknowledged or partially
// acknowledged batch yields an error so the runner refuses to advance the
// checkpoint — replay is safer than persisting an unverifiable boundary.
func (s *ClickHouseSink) SinkCommitMetadata(ctx context.Context) (map[string]any, error) {
	st := s.dedupState.Load()
	if st == nil {
		return nil, fmt.Errorf("clickhouse sink: no fully acknowledged batch since last checkpoint (write failed or empty)")
	}
	meta := map[string]any{
		"dedup_token": st.token,
		"protocol":    st.protocol,
		"batch_seq":   st.batchSeq,
		"row_count":   st.recordCount,
		"acked_at":    st.ackedAt.Format(time.RFC3339Nano),
		"dedup_mode":  "server",
	}
	if !s.insertDedupSupported.Load() {
		// Token still recorded for audit/replay equivalence, but the server
		// does not deduplicate on it; ReplacingMergeTree absorption is the
		// duplicate boundary (documented in docs/etl-idempotency.md).
		meta["dedup_mode"] = "record_only"
	}
	if len(st.tableRows) > 0 {
		tables := make(map[string]any, len(st.tableRows))
		for t, n := range st.tableRows {
			tables[t] = n
		}
		meta["tables"] = tables
	}
	return meta, nil
}

// classifyWriteError maps a clickhouse write failure into the three-state
// ack contract used by retry/DLQ decisions. The classification is shared by
// native and HTTP paths: connection-level failures after the block may have
// reached the server are Unknown (replay-safe); server-rejected payloads
// with an explicit error before data are NotAcked (retry/DLQ safe); anything
// else stays Unknown so at-least-once never degrades to at-most-once.
func classifyClickHouseWriteError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "context canceled"), strings.Contains(msg, "context deadline exceeded"),
		strings.Contains(msg, "connection reset"), strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "eof"),
		strings.Contains(msg, "unexpected packet"), strings.Contains(msg, "close"):
		// Transport torn down mid-flight: block may or may not be committed.
		return core.ClassifiedError{Class: core.ErrorClassTransient, Err: fmt.Errorf("ack unknown (transport failure after possible server commit): %w", err)}
	case strings.Contains(msg, "unknown column"), strings.Contains(msg, "doesn't exist"),
		strings.Contains(msg, "does not exist"), strings.Contains(msg, "type mismatch"),
		strings.Contains(msg, "cannot convert"), strings.Contains(msg, "too large"),
		strings.Contains(msg, "read-only"), strings.Contains(msg, "access denied"),
		strings.Contains(msg, "authentication"), strings.Contains(msg, "namespace"):
		// Server rejected before any row was written.
		return core.ClassifiedError{Class: classifyPreAckClass(msg), Err: fmt.Errorf("not acked (server rejected batch before write): %w", err)}
	default:
		return core.ClassifiedError{Class: core.ErrorClassUnknown, Err: fmt.Errorf("ack unknown: %w", err)}
	}
}

func classifyPreAckClass(msg string) core.ErrorClass {
	switch {
	case strings.Contains(msg, "access denied"), strings.Contains(msg, "authentication"), strings.Contains(msg, "read-only"):
		return core.ErrorClassAuth
	case strings.Contains(msg, "unknown column"), strings.Contains(msg, "doesn't exist"),
		strings.Contains(msg, "does not exist"), strings.Contains(msg, "type mismatch"),
		strings.Contains(msg, "cannot convert"):
		return core.ErrorClassSchema
	default:
		return core.ErrorClassData
	}
}

// withInsertDedupToken attaches the batch token to the native protocol
// context so sendQuery carries insert_dedup_token as a query setting.
func withInsertDedupToken(ctx context.Context, token string, supported bool) context.Context {
	if !supported || token == "" {
		return ctx
	}
	return clickhouseWithSettings(ctx, map[string]any{"insert_dedup_token": token})
}
