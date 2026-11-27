import type { DLQItem, RecoveryEvent } from './types';
import { validRowId, type TaskContext } from './routing';

export function dlqRowId(item: Pick<DLQItem, 'id' | 'row_id'>): string | undefined {
  if (typeof item.row_id === 'string' && validRowId(item.row_id)) return item.row_id;
  if (typeof item.id === 'number' && Number.isSafeInteger(item.id) && item.id > 0) return String(item.id);
  return undefined;
}

export function recoveryContext(event: RecoveryEvent): TaskContext {
  return {
    pipelineId: event.pipeline_id, recoveryId: event.id,
    ...(event.node_id ? { nodeId: event.node_id } : {}),
    ...(event.connection_known && event.connection_name ? { connectionName: event.connection_name } : {}),
  };
}

export function contextMatchesRecovery(context: TaskContext | undefined, event: RecoveryEvent): boolean {
  return (!context?.pipelineId || context.pipelineId === event.pipeline_id)
    && (!context?.recoveryId || context.recoveryId === event.id)
    && (!context?.nodeId || context.nodeId === event.node_id)
    && (!context?.connectionName || (event.connection_known && context.connectionName === event.connection_name));
}
