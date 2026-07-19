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

export type Pipeline = {
  id?: string;
  name: string;
  status: string;
  stats: PipelineStats;
  dag?: boolean;
  parallelism?: number;
  shard_strategy?: string;
  shard_count?: number;
  shards?: { index: number; status: string; stats: PipelineStats }[];
  tags?: string[];
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

export type TFunc = (key: string) => string;

export type ApiState<T> = {
  data?: T;
  error?: string;
  loading: boolean;
};
