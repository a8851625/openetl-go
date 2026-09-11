package backup

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const exportPageSize = 1000

// ExportFile streams a v2 snapshot to a private temporary file. Read, encode,
// inventory or write failures leave the previous destination untouched. Writers
// sharing this metadata/state must be stopped for the whole maintenance window.
func ExportFile(ctx context.Context, store storage.Storage, path string, opts Options) (storage.ObjectCounts, error) {
	var counts storage.ObjectCounts
	err := writeAtomicFile(path, func(w io.Writer) error {
		var err error
		counts, err = ExportJSON(ctx, store, w, opts)
		return err
	})
	return counts, err
}

// ExportJSON writes one page at a time, never collecting a full table or WASM
// artifact. On error w may contain an incomplete document; use ExportFile for
// atomic publication. A page and the largest individual row bound its memory.
func ExportJSON(ctx context.Context, store storage.Storage, w io.Writer, opts Options) (storage.ObjectCounts, error) {
	var counts storage.ObjectCounts
	if store == nil || w == nil {
		return counts, fmt.Errorf("backup: store and writer required")
	}
	secrets := storage.NewSecretFieldStore(store, nil).(*storage.SecretFieldStore)
	if opts.SecretFieldResolver != nil {
		secrets = storage.NewSecretFieldStore(storage.UnwrapStorage(store), nil).(*storage.SecretFieldStore)
		secrets.WithSecretFieldResolver(opts.SecretFieldResolver)
	}
	reader, ok := storage.UnwrapStorage(store).(storage.BackupReader)
	if !ok {
		return counts, fmt.Errorf("backup: %T does not support complete paged export", store)
	}
	want, err := reader.CountObjects(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: initial inventory: %w", err)
	}
	schema, err := reader.SchemaVersions(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: schema versions: %w", err)
	}
	header, err := json.Marshal(struct {
		FormatVersion int                        `json:"format_version"`
		CreatedAt     time.Time                  `json:"created_at"`
		Backend       string                     `json:"backend,omitempty"`
		Schema        []storage.SchemaVersionRow `json:"schema_versions,omitempty"`
	}{FormatVersion, time.Now().UTC(), opts.Backend, schema})
	if err != nil {
		return counts, err
	}
	out := bufio.NewWriterSize(w, 64*1024)
	if _, err := out.Write(header[:len(header)-1]); err != nil {
		return counts, err
	}
	enc := json.NewEncoder(out)
	tables := []struct {
		name  string
		count *int
	}{
		{"pipelines", &counts.Pipelines}, {"pipeline_versions", &counts.PipelineVersions},
		{"checkpoints", &counts.Checkpoints}, {"dead_letters", &counts.DeadLetters},
		{"audit_logs", &counts.AuditLogs}, {"run_history", &counts.RunHistory},
		{"workers", &counts.Workers}, {"task_assignments", &counts.Tasks},
		{"plugins", &counts.Plugins}, {"connections", &counts.Connections}, {"settings", &counts.Settings},
	}
	for _, table := range tables {
		opening, closing := "[", "]"
		if table.name == "settings" {
			opening, closing = "{", "}"
		}
		if _, err := fmt.Fprintf(out, ",\n%q:%s", table.name, opening); err != nil {
			return counts, err
		}
		err = walkBackupPages(ctx, reader, table.name, func(row storage.BackupRow) error {
			if *table.count != 0 {
				if err := out.WriteByte(','); err != nil {
					return err
				}
			}
			var value any = row.Value
			if c, ok := value.(*storage.ConnectionEntry); ok && secrets.ConnectionHasPlaintextSecrets(c) {
				return fmt.Errorf("plaintext connection/settings secrets remain; run --check-secrets and --remediate-secrets before export")
			}
			if table.name == "settings" {
				setting, ok := value.(*storage.BackupSetting)
				if !ok || setting == nil {
					return fmt.Errorf("invalid settings backup row")
				}
				if secrets.SettingHasPlaintextSecret(setting.Key, setting.Value) {
					return fmt.Errorf("plaintext connection/settings secrets remain; run --check-secrets and --remediate-secrets before export")
				}
				key, err := json.Marshal(setting.Key)
				if err != nil {
					return err
				}
				if _, err := out.Write(key); err != nil {
					return err
				}
				if err := out.WriteByte(':'); err != nil {
					return err
				}
				value = setting.Value
			}
			if err := enc.Encode(value); err != nil {
				return err
			}
			*table.count++
			return nil
		})
		if err != nil {
			return counts, fmt.Errorf("backup: export %s: %w", table.name, err)
		}
		if _, err := out.WriteString(closing); err != nil {
			return counts, err
		}
	}
	if _, err := out.WriteString(",\n\"plugin_artifacts\":{"); err != nil {
		return counts, err
	}
	artifacts := 0
	err = walkBackupPages(ctx, reader, "plugins", func(row storage.BackupRow) error {
		p, ok := row.Value.(*storage.PluginEntry)
		if !ok || p == nil {
			return fmt.Errorf("invalid plugin backup row")
		}
		if artifacts != 0 {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if err := exportArtifact(out, p, opts); err != nil {
			return err
		}
		artifacts++
		return nil
	})
	if err != nil {
		return counts, fmt.Errorf("backup: plugin artifacts: %w", err)
	}
	after, err := reader.CountObjects(ctx)
	if err != nil {
		return counts, fmt.Errorf("backup: final inventory: %w", err)
	}
	if counts != want || counts != after || artifacts != counts.Plugins {
		return counts, fmt.Errorf("backup: inventory mismatch (before=%+v exported=%+v after=%+v artifacts=%d); stop all writers and retry", want, counts, after, artifacts)
	}
	if _, err := out.WriteString("},\n\"counts\":"); err != nil {
		return counts, err
	}
	if err := enc.Encode(counts); err != nil {
		return counts, err
	}
	if _, err := out.WriteString("}\n"); err != nil {
		return counts, err
	}
	return counts, out.Flush()
}

func walkBackupPages(ctx context.Context, reader storage.BackupReader, table string, visit func(storage.BackupRow) error) error {
	var after *string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := reader.ReadBackupPage(ctx, table, after, exportPageSize)
		if err != nil {
			return err
		}
		if len(page) > exportPageSize {
			return fmt.Errorf("backend exceeded backup page size")
		}
		for _, row := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			if row.Value == nil {
				return fmt.Errorf("nil backup row")
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		if len(page) < exportPageSize {
			return nil
		}
		next := page[len(page)-1].Key
		if after != nil && next == *after {
			return fmt.Errorf("backup cursor did not advance")
		}
		after = &next
	}
}

func exportArtifact(w io.Writer, p *storage.PluginEntry, opts Options) error {
	path := p.WASMPath
	if path == "" {
		path = filepath.Join(opts.PluginsDir, p.Name+".wasm")
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("read plugin artifact %q: %w", p.Name, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("plugin artifact %q is not a regular file", p.Name)
	}
	key, err := json.Marshal(p.Name)
	if err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if _, err := io.WriteString(w, ":\""); err != nil {
		return err
	}
	encoder := base64.NewEncoder(base64.StdEncoding, w)
	n, err := io.Copy(encoder, f)
	if err != nil {
		return err
	}
	if n != info.Size() {
		return fmt.Errorf("plugin artifact %q changed size during backup", p.Name)
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	_, err = io.WriteString(w, "\"")
	return err
}
