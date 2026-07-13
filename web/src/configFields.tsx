import React from 'react';
import type { TFunc } from './types';

export type PluginSchemaField = {
  name: string;
  type: 'string' | 'int' | 'float' | 'bool' | 'string_array' | 'map';
  required?: boolean;
  default?: unknown;
  description?: string;
  secret?: boolean;
  example?: unknown;
  enum?: string[];
  scope?: 'connection' | 'behavior' | string;
  unimplemented?: boolean;
};

export type FieldScopeFilter = 'all' | 'connection' | 'behavior';

export function filterFieldsByScope(fields: PluginSchemaField[] = [], scope: FieldScopeFilter = 'all') {
  if (scope === 'all') return fields;
  return fields.filter((field) => {
    const fieldScope = field.scope || 'connection';
    return fieldScope === scope;
  });
}

export function buildDefaultConfig(fields: PluginSchemaField[] = []) {
  const next: Record<string, unknown> = {};
  fields.forEach((field) => {
    const seed = field.default !== undefined ? field.default : field.required ? field.example : undefined;
    if (seed !== undefined) next[field.name] = seed;
  });
  return next;
}

export function missingRequiredFields(fields: PluginSchemaField[] = [], config: Record<string, unknown>) {
  return fields
    .filter((field) => field.required)
    .filter((field) => {
      const value = config[field.name];
      if (value === undefined || value === null) return true;
      if (typeof value === 'string') return value.trim() === '';
      if (Array.isArray(value)) return value.length === 0;
      if (typeof value === 'object') return Object.keys(value as Record<string, unknown>).length === 0;
      return false;
    })
    .map((field) => field.name);
}

export function exampleText(value: unknown) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function parseNumber(value: string, type: PluginSchemaField['type']) {
  if (value.trim() === '') return undefined;
  const parsed = type === 'int' ? parseInt(value, 10) : parseFloat(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseMapInput(value: string) {
  if (value.trim() === '') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return value;
  }
}

function mapText(value: unknown) {
  if (value === undefined || value === null || value === '') return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value, null, 2);
}

export function ConfigForm({
  fields,
  config,
  onChange,
  t,
  emptyText,
}: {
  fields: PluginSchemaField[];
  config: Record<string, unknown>;
  onChange: (cfg: Record<string, unknown>) => void;
  t: TFunc;
  emptyText?: string;
}) {
  if (!fields || fields.length === 0) {
    return <div className="text-xs text-slate-400">{emptyText || t('conn.noConfigFields')}</div>;
  }

  const update = (name: string, value: unknown) => {
    const next = { ...config };
    if (value === undefined || value === '') {
      delete next[name];
    } else {
      next[name] = value;
    }
    onChange(next);
  };

  return (
    <div className="space-y-3">
      {fields.map((field) => {
        const value = config[field.name] ?? field.default ?? '';
        const placeholder = exampleText(field.example) || field.description || '';
        let input: React.ReactNode;
        if (field.enum && field.enum.length > 0) {
          input = (
            <select className="input w-full text-sm" value={String(value)} onChange={(e) => update(field.name, e.target.value)}>
              {!field.required && <option value="">{t('conn.leaveDefault')}</option>}
              {field.enum.map((opt) => <option key={opt} value={opt}>{opt}</option>)}
            </select>
          );
        } else if (field.type === 'bool') {
          input = (
            <label className="flex h-9 items-center gap-2">
              <input
                type="checkbox"
                checked={!!value}
                onChange={(e) => update(field.name, e.target.checked)}
                className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500"
              />
              <span className="text-xs text-slate-500">{value ? t('common.enabled') : t('common.disabled')}</span>
            </label>
          );
        } else if (field.type === 'int' || field.type === 'float') {
          input = (
            <input
              type="number"
              step={field.type === 'float' ? '0.01' : '1'}
              className="input w-full text-sm"
              value={value === undefined ? '' : String(value)}
              onChange={(e) => update(field.name, parseNumber(e.target.value, field.type))}
              placeholder={placeholder}
            />
          );
        } else if (field.type === 'string_array') {
          input = (
            <input
              className="input w-full text-sm"
              value={Array.isArray(value) ? value.join(', ') : String(value || '')}
              onChange={(e) => update(field.name, e.target.value.split(',').map((s) => s.trim()).filter(Boolean))}
              placeholder={placeholder || 'value1, value2'}
            />
          );
        } else if (field.type === 'map') {
          input = (
            <textarea
              className="input min-h-24 w-full resize-y py-2 font-mono text-xs leading-relaxed"
              value={mapText(value)}
              onChange={(e) => update(field.name, parseMapInput(e.target.value))}
              placeholder={placeholder || '{"key": "value"}'}
            />
          );
        } else {
          const multiline = ['query', 'script', 'code', 'rules', 'body'].includes(field.name);
          input = multiline ? (
            <textarea
              className="input min-h-20 w-full resize-y py-2 font-mono text-xs leading-relaxed"
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              placeholder={placeholder}
            />
          ) : (
            <input
              type={field.secret ? 'password' : 'text'}
              className="input w-full text-sm"
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              placeholder={placeholder}
            />
          );
        }

        return (
          <div key={field.name}>
            <label className="mb-1 flex items-center gap-1 text-xs font-medium text-slate-600">
              <span>{field.name}</span>
              {field.required && <span className="text-rose-500">*</span>}
              {field.secret && <span className="badge badge-amber px-1.5 py-0 text-[10px]">{t('ui.secret')}</span>}
              {field.scope === 'behavior' && <span className="badge badge-slate px-1.5 py-0 text-[10px]">{t('field.scopeBehavior')}</span>}
              {field.scope === 'connection' && <span className="badge badge-blue px-1.5 py-0 text-[10px]">{t('field.scopeConnection')}</span>}
              {field.default !== undefined && <span className="ml-auto text-[10px] text-slate-400">{t('conn.default')}: {exampleText(field.default)}</span>}
            </label>
            {input}
            {field.description && <div className="mt-1 text-xs text-slate-400">{field.description}</div>}
          </div>
        );
      })}
    </div>
  );
}
