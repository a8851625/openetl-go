package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// ── Table Mapping Processor ──────────────────────────────────────────

// TableMappingProcessor rewrites rec.Metadata.Table based on the pipeline's
// table_mapping config. This is a pipeline-level RecordProcessor that runs
// BEFORE user transforms.
type TableMappingProcessor struct {
	mapping *TableMapping
}

func NewTableMappingProcessor(tm *TableMapping) *TableMappingProcessor {
	return &TableMappingProcessor{mapping: tm}
}

func (p *TableMappingProcessor) Name() string { return "table_mapping" }

func (p *TableMappingProcessor) Process(ctx context.Context, rec core.Record) (core.Record, error) {
	if p.mapping == nil || rec.Metadata.Table == "" {
		return rec, nil
	}
	rec.Metadata.Table = p.mapping.MapTable(rec.Metadata.Table)
	return rec, nil
}

// ── Field Enrichment Processor ───────────────────────────────────────

// FieldEnrichmentProcessor adds system fields to every record.
// This centralizes the "add pipeline metadata fields" pattern so any sink
// can rely on standard fields being present.
//
// Fields added:
//   - _pipeline: pipeline name
//   - _source_table: original source table (before mapping)
//   - _ingested_at: ingestion timestamp
type FieldEnrichmentProcessor struct {
	pipelineName string
	addFields    map[string]any
}

func NewFieldEnrichmentProcessor(pipelineName string, extraFields map[string]any) *FieldEnrichmentProcessor {
	return &FieldEnrichmentProcessor{
		pipelineName: pipelineName,
		addFields:    extraFields,
	}
}

func (p *FieldEnrichmentProcessor) Name() string { return "field_enrichment" }

func (p *FieldEnrichmentProcessor) Process(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		rec.Data = make(map[string]any)
	}
	// Only add enrichment fields if they don't already exist in the record.
	if _, ok := rec.Data["_pipeline"]; !ok {
		rec.Data["_pipeline"] = p.pipelineName
	}
	if rec.Metadata.Table != "" {
		if _, ok := rec.Data["_source_table"]; !ok {
			rec.Data["_source_table"] = rec.Metadata.Table
		}
	}
	if _, ok := rec.Data["_ingested_at"]; !ok {
		rec.Data["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
	}
	// Add custom extra fields.
	for k, v := range p.addFields {
		if _, ok := rec.Data[k]; !ok {
			rec.Data[k] = v
		}
	}
	return rec, nil
}

// ── Data Masking Processor ───────────────────────────────────────────

// DataMaskingProcessor applies field-level masking for sensitive data.
// This is a reusable processor that can be configured per-pipeline.
//
// Config example:
//
//	data_masking:
//	  fields:
//	    phone: "mask_phone"      # 13812345678 → 138****5678
//	    email: "mask_email"      # user@example.com → u***@example.com
//	    id_card: "mask_id_card"  # 110101199001011234 → 110***********1234
//	    ssn: "mask_all"          # any value → ********
type DataMaskingProcessor struct {
	rules map[string]string // field → mask strategy
}

func NewDataMaskingProcessor(rules map[string]string) *DataMaskingProcessor {
	return &DataMaskingProcessor{rules: rules}
}

func (p *DataMaskingProcessor) Name() string { return "data_masking" }

func (p *DataMaskingProcessor) Process(ctx context.Context, rec core.Record) (core.Record, error) {
	if len(p.rules) == 0 || rec.Data == nil {
		return rec, nil
	}
	for field, strategy := range p.rules {
		val, ok := rec.Data[field]
		if !ok {
			continue
		}
		str, ok := val.(string)
		if !ok {
			str = fmt.Sprintf("%v", val)
		}
		rec.Data[field] = applyMask(str, strategy)
	}
	return rec, nil
}

func applyMask(value, strategy string) string {
	switch strategy {
	case "mask_phone":
		if len(value) >= 7 {
			return value[:3] + "****" + value[len(value)-4:]
		}
		return "****"
	case "mask_email":
		at := strings.Index(value, "@")
		if at > 1 {
			return string(value[0]) + "***" + value[at:]
		}
		return "****"
	case "mask_id_card":
		if len(value) >= 14 {
			return value[:3] + "***********" + value[len(value)-4:]
		}
		return "****"
	case "mask_all":
		return "********"
	case "hash":
		return fmt.Sprintf("%x", simpleHash(value))
	default:
		return value
	}
}

func simpleHash(s string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range s {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

// ── Processor Builder ────────────────────────────────────────────────

// BuildProcessors constructs the RecordProcessorChain from the pipeline spec.
// This runs BEFORE transforms and handles pipeline-level concerns.
func BuildProcessors(spec *Spec) core.RecordProcessorChain {
	var chain core.RecordProcessorChain

	// 1. Table mapping (if configured).
	if spec.TableMapping != nil {
		chain = append(chain, NewTableMappingProcessor(spec.TableMapping))
	}

	// 2. Data masking (if configured via spec.Hooks or a dedicated field).
	// This can be extended to read from a dedicated data_masking spec field.

	return chain
}
