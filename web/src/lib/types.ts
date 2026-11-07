// 共享领域类型（从 main.tsx 抽出，业务契约保持不变）

export type PipelineStats = {
  records_read: number;
  records_written: number;
  records_failed: number;
  records_dlq: number;
  last_error?: string;
  last_checkpoint?: string;
  started_at?: string;
  uptime?: string;
  bytes_read?: number;
  bytes_written?: number;
  dlq_replay_count?: number;
  dlq_delete_count?: number;
  last_error_code?: string;
  last_error_remediation?: string;
};

export type MetricsPipeline = PipelineStats & {
  id?: string;
  name: string;
  status: string;
  checkpoint_age_seconds: number;
  source_read_latency_ms: number;
  sink_write_latency_ms: number;
  last_batch_size: number;
  avg_batch_size: number;
  batch_count: number;
  cdc_lag_ms: number;
  dlq_file_count: number;
};

export type SpecSummary = {
  source?: string;
  source_mode?: 'cdc' | 'batch' | 'streaming' | 'scheduled' | 'dag';
  transforms?: string[];
  sink?: string;
  write_mode?: string;
  schedule?: string;
  parallel_shards?: number;
  table_mapping?: boolean;
  has_dlq?: boolean;
  allow_unsafe?: boolean;
  dag_sources?: string[];
  dag_transforms?: string[];
  dag_sinks?: string[];
};

export type Pipeline = {
  id?: string;
  name: string;
  status: string;
  stats: PipelineStats;
  /**
   * dlq_backlog is the CURRENT dead-letter backlog reported by the server.
   * stats.records_dlq is a cumulative per-process counter (it resets on restart
   * and never decreases after replay), so it must never be rendered as backlog.
   * dlq_backlog_known=false means the count could not be read (unknown, not 0).
   */
  dlq_backlog?: number;
  dlq_backlog_known?: boolean;
  dag?: boolean;
  parallelism?: number;
  shard_strategy?: string;
  shard_count?: number;
  shards?: { index: number; status: string; stats: PipelineStats }[];
  tags?: string[];
  spec_summary?: SpecSummary;
};

export type ShardInfo = {
  index: number;
  status: string;
  stats: PipelineStats;
  logs?: PipelineLogEntry[];
  logs_last_seq?: number;
};

export type PluginInfo = {
  required?: string[];
  capabilities?: string[];
  maturity?: string;
};

export type PluginResponse = {
  sources: string[];
  sinks: string[];
  transforms: string[];
  metadata?: Record<string, Record<string, PluginInfo>>;
};

export type Checkpoint = {
  id: string;
  job_name: string;
  source: string;
  position: unknown;
  timestamp: string;
};

export type DLQIdentityContext = {
  replay_provenance?: string;
  replay_state?: string;
  replay_attempt?: number;
  replay_error?: string;
  failure_reason?: string;
  missing_columns?: string[] | null;
  sink_acknowledged_at?: string;
};

export type DLQItem = {
  id?: number;
  job_name: string;
  record: { operation: string; data: Record<string, unknown> };
  error: string;
  timestamp: string;
  error_class?: string;
  record_hash?: string;
  pipeline_version?: number;
  dag_node?: string;
  attempt?: number;
  identity_context?: DLQIdentityContext;
};

export type AuditEvent = {
  timestamp?: string;
  action?: string;
  target?: string;
  method?: string;
  path?: string;
};

export type PipelineLogEntry = {
  timestamp: string;
  message: string;
  level: string;
  seq: number;
};

export type PipelineVersion = {
  id: number;
  pipeline: string;
  version: number;
  spec_yaml: string;
  created_at: string;
};

export type DAGNode = {
  id: string;
  kind: string;
  plugin: string;
  config?: Record<string, unknown>;
  x?: number;
  y?: number;
};

export type DAGEdge = {
  id?: string;
  from: string;
  to: string;
  condition?: { field: string; operator: string; value: unknown };
};

export type DAGResponse = {
  dag: { nodes: DAGNode[]; edges: DAGEdge[] };
  node_configs: { id: string; kind: string; plugin: string; config?: Record<string, unknown> }[];
  schedule?: { type: string; cron?: string };
  execution?: Record<string, unknown>;
  retry?: Record<string, unknown>;
};

export type PreviewResponse = {
  stages: Record<string, PipelineLogEntry[]>;
  shard_logs: { shard: number; entries: PipelineLogEntry[] }[];
  total_logs: number;
};

export type ConnectionEntry = {
  name: string;
  kind: 'source' | 'sink' | 'transform';
  type: string;
  last_status?: string;
  last_error?: string;
  last_tested_at?: string;
  config?: Record<string, unknown>;
};

export type ConnectionContext = {
  connection?: ConnectionEntry;
  recommendations?: { field: string; value: unknown; reason: string }[];
  introspection?: {
    ok?: boolean;
    status?: string;
    error?: string;
    databases?: string[];
    tables?: {
      name: string;
      database?: string;
      schema?: string;
      columns?: { name: string; data_type?: string; nullable?: boolean }[];
      primary_key?: string[];
    }[];
    topics?: {
      name: string;
      partitions?: { id: number; oldest_offset?: number; newest_offset?: number; leader?: number }[];
    }[];
    targets?: {
      kind: string;
      location: string;
      prefix?: string;
      format?: string;
      exists?: boolean;
      writable?: boolean;
    }[];
    schema?: { name: string; data_type?: string; nullable?: boolean }[];
    sample?: Record<string, unknown>[];
    warnings?: string[];
    checked_at?: string;
  };
};

export type ConnectionRecommendation = {
  field: string;
  value: unknown;
  reason: string;
};

// Data reconciliation (POST /api/v2/pipelines/{id}/reconcile). The report is a
// point-in-time comparison, not an exactly-once proof: read `limits` before
// acting on a verdict.
export type ReconcileBucketMismatch = {
  bucket: number;
  source_rows: number;
  target_rows: number;
  source_digest?: string;
  target_digest?: string;
};

export type ReconcileRowDiffKind = 'missing_in_target' | 'extra_in_target' | 'value_mismatch';

export type ReconcileRowDiff = {
  key: string;
  kind: ReconcileRowDiffKind;
  source_row?: string;
  target_row?: string;
};

export type ReconcileTableReport = {
  source_table: string;
  target_table: string;
  verdict: string;
  source_rows: number;
  target_rows: number;
  row_delta: number;
  source_digest?: string;
  target_digest?: string;
  compared_columns?: string[];
  target_columns_used?: string[];
  missing_target_columns?: string[];
  extra_target_columns?: string[];
  bucket_key?: string;
  buckets?: number;
  bucket_mismatches?: ReconcileBucketMismatch[];
  bucket_scan_truncated?: boolean;
  row_diff_scope?: string;
  row_diffs?: ReconcileRowDiff[];
  row_diff_truncated?: boolean;
  detail?: string;
  duration_ms?: number;
};

export type ReconcileSideInfo = {
  role: string;
  type: string;
  dialect: string;
  database: string;
  endpoint?: string;
};

export type ReconcileReport = {
  pipeline_id: string;
  pipeline_name?: string;
  pipeline_status?: string;
  source: ReconcileSideInfo;
  target: ReconcileSideInfo;
  deep: boolean;
  buckets: number;
  tables: ReconcileTableReport[];
  summary: {
    tables: number;
    matched: number;
    count_mismatch: number;
    content_mismatch: number;
    schema_mismatch: number;
    errors: number;
    unsupported: number;
  };
  verdict: string;
  limits?: string[];
  started_at?: string;
  finished_at?: string;
  duration_ms?: number;
};

export type ReconcileCapabilities = {
  supported?: boolean;
  source_types?: string[];
  sink_types?: string[];
  verdicts?: string[];
  limits?: string[];
  [key: string]: unknown;
};

// Single-table re-snapshot (POST /api/v2/pipelines/{id}/resync).
export type ResyncResult = {
  status?: string;
  generation?: number;
  tables?: string[];
  details?: (string | Record<string, unknown>)[];
  semantics?: Record<string, unknown>;
};

export type TFunc = (key: string) => string;

export type ApiState<T> = {
  data?: T;
  error?: string;
  /** True for the first request, when a page has no data to render yet. */
  loading: boolean;
  /** True for every in-flight request, including background/manual refreshes. */
  refreshing?: boolean;
};
