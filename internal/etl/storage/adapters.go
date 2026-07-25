package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// ── CheckpointStore adapter ──────────────────────────────────────────

// CheckpointStoreAdapter bridges storage.Storage to core.CheckpointStore.
type CheckpointStoreAdapter struct {
	store Storage
}

func NewCheckpointStoreAdapter(s Storage) *CheckpointStoreAdapter {
	return &CheckpointStoreAdapter{store: s}
}

func (a *CheckpointStoreAdapter) Save(ctx context.Context, cp core.Checkpoint) error {
	rec := &CheckpointRecord{
		JobName:   cp.JobName,
		Source:    cp.Source,
		Position:  cp.Position,
		Timestamp: time.Now(),
	}
	return a.store.SaveCheckpoint(ctx, rec)
}

func (a *CheckpointStoreAdapter) Load(ctx context.Context, jobName string) (*core.Checkpoint, error) {
	rec, err := a.store.LoadCheckpoint(ctx, jobName)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return &core.Checkpoint{
		ID:        jobName,
		JobName:   rec.JobName,
		Source:    rec.Source,
		Position:  rec.Position,
		Timestamp: rec.Timestamp,
	}, nil
}

func (a *CheckpointStoreAdapter) Delete(ctx context.Context, jobName string) error {
	return a.store.DeleteCheckpoint(ctx, jobName)
}

func (a *CheckpointStoreAdapter) List(ctx context.Context) ([]core.Checkpoint, error) {
	recs, err := a.store.ListCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]core.Checkpoint, len(recs))
	for i, rec := range recs {
		result[i] = core.Checkpoint{
			ID:        rec.JobName,
			JobName:   rec.JobName,
			Source:    rec.Source,
			Position:  rec.Position,
			Timestamp: rec.Timestamp,
		}
	}
	return result, nil
}

// ── DLQWriter adapter ────────────────────────────────────────────────

// DLQWriterAdapter bridges storage.Storage to dlq.Writer-like interface.
// It also provides the filter/list/delete capabilities that the SQL-backed
// store enables natively.
type DLQWriterAdapter struct {
	store Storage
}

func NewDLQWriterAdapter(s Storage) *DLQWriterAdapter {
	return &DLQWriterAdapter{store: s}
}

// Write persists a dead-letter record into the database.
func (a *DLQWriterAdapter) Write(ctx context.Context, jobName string, record core.Record, errMsg, errClass string, attempt int) error {
	return a.WriteEntry(ctx, pipeline.DLQEntry{
		JobName:    jobName,
		Record:     record,
		Error:      errMsg,
		ErrorClass: errClass,
		Attempt:    attempt,
	})
}

// WriteEntry persists a pipeline-level dead-letter entry into the database.
func (a *DLQWriterAdapter) WriteEntry(ctx context.Context, entry pipeline.DLQEntry) error {
	hash, err := RecordHash(entry.Record)
	if err != nil {
		return fmt.Errorf("hash dlq record: %w", err)
	}
	rec := &DLQRecord{
		JobName:         entry.JobName,
		Record:          entry.Record,
		Error:           entry.Error,
		ErrorClass:      entry.ErrorClass,
		Attempt:         entry.Attempt,
		RecordHash:      hash,
		PipelineVersion: entry.PipelineVersion,
		DAGNode:         entry.DAGNode,
		CreatedAt:       time.Now(),
	}
	return a.store.WriteDeadLetter(ctx, rec)
}

// Read returns the most recent dead-letter records for a job (limit <=0 means 100).
func (a *DLQWriterAdapter) Read(ctx context.Context, jobName string, limit int) ([]DLQRecord, error) {
	if limit <= 0 {
		limit = 10000
	}
	recs, err := a.store.ListDeadLetters(ctx, DLQFilter{JobName: jobName, Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]DLQRecord, len(recs))
	for i, rec := range recs {
		result[i] = *rec
	}
	return result, nil
}

// ReadByID returns one dead-letter record by primary key, scoped to a pipeline.
func (a *DLQWriterAdapter) ReadByID(ctx context.Context, jobName string, id int64) (*DLQRecord, error) {
	return a.store.GetDeadLetterByID(ctx, jobName, id)
}

// ReadFiltered returns dead-letter records matching the filter criteria.
func (a *DLQWriterAdapter) ReadFiltered(ctx context.Context, filter DLQFilter) ([]DLQRecord, error) {
	recs, err := a.store.ListDeadLetters(ctx, filter)
	if err != nil {
		return nil, err
	}
	result := make([]DLQRecord, len(recs))
	for i, rec := range recs {
		result[i] = *rec
	}
	return result, nil
}

// DeleteAll removes all dead-letter records for a job.
func (a *DLQWriterAdapter) DeleteAll(ctx context.Context, jobName string) error {
	return a.store.DeleteAllDeadLetters(ctx, jobName)
}

// DeleteByFilter removes dead-letter records matching the filter and returns the count deleted.
func (a *DLQWriterAdapter) DeleteByFilter(ctx context.Context, filter DLQFilter) (int64, error) {
	return a.store.DeleteDeadLettersByFilter(ctx, filter)
}

// DeleteByID removes a single dead-letter record by ID.
func (a *DLQWriterAdapter) DeleteByID(ctx context.Context, id int64) error {
	return a.store.DeleteDeadLetterByID(ctx, id)
}

// Count returns the number of dead-letter records for a job. Uses COUNT(*) on
// the storage backend (avoids loading up to 100k rows into memory).
func (a *DLQWriterAdapter) Count(ctx context.Context, jobName string) int {
	n, err := a.store.CountDeadLetters(ctx, jobName)
	if err != nil {
		return 0
	}
	return int(n)
}

// ── AuditWriter adapter ──────────────────────────────────────────────

// AuditWriterAdapter bridges storage.Storage for audit logging.
type AuditWriterAdapter struct {
	store Storage
}

func NewAuditWriterAdapter(s Storage) *AuditWriterAdapter {
	return &AuditWriterAdapter{store: s}
}

func (a *AuditWriterAdapter) Write(ctx context.Context, action, method, path, target, remote string) error {
	entry := &AuditEntry{
		Action: action,
		Method: method,
		Path:   path,
		Target: target,
		Remote: remote,
	}
	return a.store.WriteAudit(ctx, entry)
}

func (a *AuditWriterAdapter) List(ctx context.Context, limit int) ([]*AuditEntry, error) {
	return a.store.ListAudit(ctx, limit)
}

// ── PipelineSpecStore adapter ────────────────────────────────────────

// PipelineSpecStore provides YAML spec persistence on top of Storage.
type PipelineSpecStore struct {
	store  Storage
	cipher *SpecCipher
}

func NewPipelineSpecStore(s Storage, ciphers ...*SpecCipher) *PipelineSpecStore {
	var cipher *SpecCipher
	if len(ciphers) > 0 {
		cipher = ciphers[0]
	}
	return &PipelineSpecStore{store: s, cipher: cipher}
}

// WithCipher returns a view over the same storage backend using the supplied
// crypto policy. It keeps the legacy one-argument constructor source-compatible
// while allowing server/worker startup to inject one validated cipher.
func (p *PipelineSpecStore) WithCipher(cipher *SpecCipher) *PipelineSpecStore {
	if p == nil {
		return nil
	}
	return &PipelineSpecStore{store: p.store, cipher: cipher}
}

func (p *PipelineSpecStore) Save(ctx context.Context, name, specYAML, status string) error {
	return p.SaveWithID(ctx, "", name, specYAML, status)
}

func (p *PipelineSpecStore) SaveWithID(ctx context.Context, id, name, specYAML, status string) error {
	return p.SaveWithIDAndCheckpointReset(ctx, id, name, specYAML, status, false)
}

func (p *PipelineSpecStore) SaveWithIDAndCheckpointReset(ctx context.Context, id, name, specYAML, status string, resetCheckpoint bool) error {
	storedYAML, err := p.encrypt(specYAML)
	if err != nil {
		return err
	}
	row := &PipelineRow{ID: id, Name: name, SpecYAML: storedYAML, Status: status}
	if atomicStore, ok := p.store.(interface {
		SavePipelineWithVersionAndCheckpointReset(context.Context, *PipelineRow, string, bool) error
	}); ok {
		return atomicStore.SavePipelineWithVersionAndCheckpointReset(ctx, row, storedYAML, resetCheckpoint)
	}
	if atomicStore, ok := p.store.(interface {
		SavePipelineWithVersion(context.Context, *PipelineRow, string) error
	}); ok {
		if err := atomicStore.SavePipelineWithVersion(ctx, row, storedYAML); err != nil {
			return err
		}
		if resetCheckpoint {
			return p.store.DeleteCheckpoint(ctx, row.ID)
		}
		return nil
	}
	if err := p.store.SavePipeline(ctx, row); err != nil {
		return err
	}
	_, err = p.store.SavePipelineVersion(ctx, row.ID, storedYAML)
	if err != nil {
		return err
	}
	if resetCheckpoint {
		return p.store.DeleteCheckpoint(ctx, row.ID)
	}
	return nil
}

// SaveCurrentWithID updates only the current pipeline row. It is used for
// compatibility repairs (for example assigning an ID to a legacy row) where
// creating a new historical version during restore would be misleading.
func (p *PipelineSpecStore) SaveCurrentWithID(ctx context.Context, id, name, specYAML, status string) error {
	storedYAML, err := p.encrypt(specYAML)
	if err != nil {
		return err
	}
	return p.store.SavePipeline(ctx, &PipelineRow{ID: id, Name: name, SpecYAML: storedYAML, Status: status})
}

func (p *PipelineSpecStore) Get(ctx context.Context, name string) (string, error) {
	row, err := p.GetRow(ctx, name)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	return row.SpecYAML, nil
}

func (p *PipelineSpecStore) GetRow(ctx context.Context, name string) (*PipelineRow, error) {
	row, err := p.store.GetPipeline(ctx, name)
	if err != nil || row == nil {
		return row, err
	}
	return p.decryptRow(row)
}

func (p *PipelineSpecStore) List(ctx context.Context) ([]*PipelineRow, error) {
	rows, err := p.store.ListPipelines(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*PipelineRow, 0, len(rows))
	for _, row := range rows {
		decrypted, err := p.decryptRow(row)
		if err != nil {
			ref := "<unknown>"
			if row != nil {
				ref = row.ID
				if ref == "" {
					ref = row.Name
				}
			}
			return nil, fmt.Errorf("decrypt pipeline %s: %w", ref, err)
		}
		result = append(result, decrypted)
	}
	return result, nil
}

func (p *PipelineSpecStore) Delete(ctx context.Context, name string) error {
	if atomicStore, ok := p.store.(interface {
		DeletePipelineWithCheckpoint(context.Context, string) error
	}); ok {
		return atomicStore.DeletePipelineWithCheckpoint(ctx, name)
	}
	return p.store.DeletePipeline(ctx, name)
}

func (p *PipelineSpecStore) Versions(ctx context.Context, name string) ([]*PipelineVersion, error) {
	refs, err := p.versionRefs(ctx, name)
	if err != nil {
		return nil, err
	}
	all := make([]*PipelineVersion, 0)
	seen := make(map[string]struct{})
	for _, ref := range refs {
		versions, listErr := p.store.ListPipelineVersions(ctx, ref)
		if listErr != nil {
			return nil, listErr
		}
		for _, version := range versions {
			if version == nil {
				continue
			}
			key := fmt.Sprintf("%d:%d", version.ID, version.Version)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			all = append(all, version)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Version != all[j].Version {
			return all[i].Version > all[j].Version
		}
		return all[i].CreatedAt.After(all[j].CreatedAt)
	})
	result := make([]*PipelineVersion, 0, len(all))
	for _, version := range all {
		decrypted, err := p.decryptVersion(version)
		if err != nil {
			versionNumber := 0
			if version != nil {
				versionNumber = version.Version
			}
			return nil, fmt.Errorf("decrypt pipeline %s version %d: %w", name, versionNumber, err)
		}
		result = append(result, decrypted)
	}
	return result, nil
}

func (p *PipelineSpecStore) GetVersion(ctx context.Context, name string, version int) (*PipelineVersion, error) {
	refs, err := p.versionRefs(ctx, name)
	if err != nil {
		return nil, err
	}
	var v *PipelineVersion
	for _, ref := range refs {
		v, err = p.store.GetPipelineVersion(ctx, ref, version)
		if err != nil {
			return nil, err
		}
		if v != nil {
			break
		}
	}
	if v == nil {
		return nil, nil
	}
	decrypted, err := p.decryptVersion(v)
	if err != nil {
		return nil, fmt.Errorf("decrypt pipeline %s version %d: %w", name, version, err)
	}
	return decrypted, nil
}

func (p *PipelineSpecStore) versionRefs(ctx context.Context, name string) ([]string, error) {
	refs := []string{name}
	row, err := p.store.GetPipeline(ctx, name)
	if err != nil {
		return nil, err
	}
	if row != nil && row.Name != "" && row.Name != name {
		refs = append(refs, row.Name)
	}
	return refs, nil
}

// ValidateReadable checks both current rows and historical versions before a
// process starts serving work. Crypto failures therefore stop startup instead
// of surfacing later as a skipped pipeline or a broken rollback request.
func (p *PipelineSpecStore) ValidateReadable(ctx context.Context) error {
	rows, err := p.List(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		ref := row.ID
		if ref == "" {
			ref = row.Name
		}
		if _, err := p.Versions(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

func (p *PipelineSpecStore) Cipher() *SpecCipher {
	return p.cipher
}

func (p *PipelineSpecStore) encrypt(specYAML string) (string, error) {
	if p.cipher == nil {
		return specYAML, nil
	}
	return p.cipher.Encrypt(specYAML)
}

func (p *PipelineSpecStore) decrypt(specYAML string) (string, error) {
	if p.cipher == nil {
		return specYAML, nil
	}
	return p.cipher.Decrypt(specYAML)
}

func (p *PipelineSpecStore) decryptRow(row *PipelineRow) (*PipelineRow, error) {
	if row == nil {
		return nil, nil
	}
	specYAML, err := p.decrypt(row.SpecYAML)
	if err != nil {
		return nil, err
	}
	copy := *row
	copy.SpecYAML = specYAML
	return &copy, nil
}

func (p *PipelineSpecStore) decryptVersion(version *PipelineVersion) (*PipelineVersion, error) {
	if version == nil {
		return nil, nil
	}
	specYAML, err := p.decrypt(version.SpecYAML)
	if err != nil {
		return nil, err
	}
	copy := *version
	copy.SpecYAML = specYAML
	return &copy, nil
}

// MarshalCheckpointPosition is a helper for serializing checkpoint positions.
func MarshalCheckpointPosition(v any) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint position: %w", err)
	}
	return json.RawMessage(data), nil
}

// ── SecretFieldStore adapter ─────────────────────────────────────────

// SecretFieldStore wraps Storage and encrypts connection/settings secret
// fields at rest while exposing decrypted values to runtime callers.
// API masking remains a separate presentation concern in the control plane.
type SecretFieldStore struct {
	Storage
	cipher *SpecCipher
}

// NewSecretFieldStore returns a storage view that applies field-level secret
// encryption. A nil or disabled cipher keeps development/legacy plaintext
// writes, but encrypted rows still fail closed on read without a matching key.
func NewSecretFieldStore(inner Storage, cipher *SpecCipher) Storage {
	if inner == nil {
		return nil
	}
	if _, ok := inner.(*SecretFieldStore); ok {
		return inner
	}
	return &SecretFieldStore{Storage: inner, cipher: cipher}
}

// Cipher exposes the configured field cipher for rotation helpers/tests.
func (s *SecretFieldStore) Cipher() *SpecCipher {
	if s == nil {
		return nil
	}
	return s.cipher
}

// SavePipelineWithVersion forwards the optional atomic current/version write
// so PipelineSpecStore type assertions keep working through this wrapper.
func (s *SecretFieldStore) SavePipelineWithVersion(ctx context.Context, row *PipelineRow, specYAML string) error {
	if atomicStore, ok := s.Storage.(interface {
		SavePipelineWithVersion(context.Context, *PipelineRow, string) error
	}); ok {
		return atomicStore.SavePipelineWithVersion(ctx, row, specYAML)
	}
	if err := s.Storage.SavePipeline(ctx, row); err != nil {
		return err
	}
	_, err := s.Storage.SavePipelineVersion(ctx, row.ID, specYAML)
	return err
}

// SavePipelineWithVersionAndCheckpointReset forwards the optional atomic write
// that also resets the checkpoint in the same transaction.
func (s *SecretFieldStore) SavePipelineWithVersionAndCheckpointReset(ctx context.Context, row *PipelineRow, specYAML string, resetCheckpoint bool) error {
	if atomicStore, ok := s.Storage.(interface {
		SavePipelineWithVersionAndCheckpointReset(context.Context, *PipelineRow, string, bool) error
	}); ok {
		return atomicStore.SavePipelineWithVersionAndCheckpointReset(ctx, row, specYAML, resetCheckpoint)
	}
	if err := s.SavePipelineWithVersion(ctx, row, specYAML); err != nil {
		return err
	}
	if resetCheckpoint {
		return s.Storage.DeleteCheckpoint(ctx, row.ID)
	}
	return nil
}

// DeletePipelineWithCheckpoint forwards the optional atomic delete boundary.
func (s *SecretFieldStore) DeletePipelineWithCheckpoint(ctx context.Context, ref string) error {
	if atomicStore, ok := s.Storage.(interface {
		DeletePipelineWithCheckpoint(context.Context, string) error
	}); ok {
		return atomicStore.DeletePipelineWithCheckpoint(ctx, ref)
	}
	if err := s.Storage.DeletePipeline(ctx, ref); err != nil {
		return err
	}
	return s.Storage.DeleteCheckpoint(ctx, ref)
}

func (s *SecretFieldStore) SaveConnection(ctx context.Context, c *ConnectionEntry) error {
	if c == nil {
		return fmt.Errorf("connection entry is nil")
	}
	stored := *c
	cfg, err := EncryptConfigSecrets(s.cipher, c.Config)
	if err != nil {
		return err
	}
	stored.Config = cfg
	return s.Storage.SaveConnection(ctx, &stored)
}

func (s *SecretFieldStore) GetConnection(ctx context.Context, name string) (*ConnectionEntry, error) {
	c, err := s.Storage.GetConnection(ctx, name)
	if err != nil || c == nil {
		return c, err
	}
	return s.decryptConnection(c)
}

func (s *SecretFieldStore) ListConnections(ctx context.Context) ([]*ConnectionEntry, error) {
	list, err := s.Storage.ListConnections(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*ConnectionEntry, 0, len(list))
	for _, c := range list {
		dec, err := s.decryptConnection(c)
		if err != nil {
			return nil, err
		}
		out = append(out, dec)
	}
	return out, nil
}

func (s *SecretFieldStore) GetSetting(ctx context.Context, key string) (string, error) {
	val, err := s.Storage.GetSetting(ctx, key)
	if err != nil || val == "" {
		return val, err
	}
	return DecryptSettingValue(s.cipher, key, val)
}

func (s *SecretFieldStore) SetSetting(ctx context.Context, key, value string) error {
	stored, err := EncryptSettingValue(s.cipher, key, value)
	if err != nil {
		return err
	}
	return s.Storage.SetSetting(ctx, key, stored)
}

func (s *SecretFieldStore) ListSettings(ctx context.Context) (map[string]string, error) {
	all, err := s.Storage.ListSettings(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(all))
	for k, v := range all {
		plain, err := DecryptSettingValue(s.cipher, k, v)
		if err != nil {
			return nil, err
		}
		out[k] = plain
	}
	return out, nil
}

// ── RetentionPurger / DB() forwarding (PR-1.3) ───────────────────────

func (s *SecretFieldStore) PurgeAuditBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if p, ok := s.Storage.(RetentionPurger); ok {
		return p.PurgeAuditBefore(ctx, cutoff, limit)
	}
	return 0, fmt.Errorf("storage backend does not support audit purge")
}

func (s *SecretFieldStore) PurgeRunHistoryBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if p, ok := s.Storage.(RetentionPurger); ok {
		return p.PurgeRunHistoryBefore(ctx, cutoff, limit)
	}
	return 0, fmt.Errorf("storage backend does not support run-history purge")
}

func (s *SecretFieldStore) PurgeFinishedTasksBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if p, ok := s.Storage.(RetentionPurger); ok {
		return p.PurgeFinishedTasksBefore(ctx, cutoff, limit)
	}
	return 0, fmt.Errorf("storage backend does not support task purge")
}

func (s *SecretFieldStore) CountObjects(ctx context.Context) (ObjectCounts, error) {
	if p, ok := s.Storage.(RetentionPurger); ok {
		return p.CountObjects(ctx)
	}
	return ObjectCounts{}, fmt.Errorf("storage backend does not support object counts")
}

func (s *SecretFieldStore) SchemaVersions(ctx context.Context) ([]SchemaVersionRow, error) {
	if p, ok := s.Storage.(RetentionPurger); ok {
		return p.SchemaVersions(ctx)
	}
	return nil, fmt.Errorf("storage backend does not support schema versions")
}

// DB exposes the underlying *sql.DB when the inner store does, so backup wipe
// can clear tables through the SecretFieldStore wrapper.
func (s *SecretFieldStore) DB() *sql.DB {
	if p, ok := s.Storage.(interface{ DB() *sql.DB }); ok {
		return p.DB()
	}
	return nil
}

// ReencryptSecrets rewrites every connection secret field and secret setting
// with the current key. Call during a controlled rotation while previous keys
// remain configured for decryption.
func (s *SecretFieldStore) ReencryptSecrets(ctx context.Context) error {
	if s == nil || s.cipher == nil || !s.cipher.Enabled() {
		return fmt.Errorf("%w; set ETL_SPEC_ENCRYPTION_KEY before re-encrypting secrets", ErrSpecEncryptionKeyUnavailable)
	}
	conns, err := s.ListConnections(ctx)
	if err != nil {
		return err
	}
	for _, c := range conns {
		if err := s.SaveConnection(ctx, c); err != nil {
			return fmt.Errorf("re-encrypt connection %q: %w", c.Name, err)
		}
	}
	settings, err := s.ListSettings(ctx)
	if err != nil {
		return err
	}
	for k, v := range settings {
		if !IsSecretFieldKey(k) || v == "" {
			continue
		}
		if err := s.SetSetting(ctx, k, v); err != nil {
			return fmt.Errorf("re-encrypt setting %q: %w", k, err)
		}
	}
	return nil
}

func (s *SecretFieldStore) decryptConnection(c *ConnectionEntry) (*ConnectionEntry, error) {
	if c == nil {
		return nil, nil
	}
	cfg, err := DecryptConfigSecrets(s.cipher, c.Config)
	if err != nil {
		return nil, err
	}
	copy := *c
	copy.Config = cfg
	return &copy, nil
}

// UnwrapStorage returns the innermost non-wrapper Storage implementation.
// Useful for dump scanners and raw SQL assertions in tests.
func UnwrapStorage(s Storage) Storage {
	for {
		sf, ok := s.(*SecretFieldStore)
		if !ok {
			return s
		}
		s = sf.Storage
	}
}
