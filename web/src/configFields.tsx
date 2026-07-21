import React from 'react';
import type { TFunc } from './types';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';

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

const selectClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring';

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
    return <div className="text-xs text-muted-foreground">{emptyText || t('conn.noConfigFields')}</div>;
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
            <select
              className={selectClass}
              value={String(value)}
              onChange={(e) => update(field.name, e.target.value)}
            >
              {!field.required && <option value="">{t('conn.leaveDefault')}</option>}
              {field.enum.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          );
        } else if (field.type === 'bool') {
          input = (
            <div className="flex h-9 items-center gap-2">
              <Switch checked={!!value} onCheckedChange={(checked) => update(field.name, checked)} />
              <span className="text-xs text-muted-foreground">
                {value ? t('common.enabled') : t('common.disabled')}
              </span>
            </div>
          );
        } else if (field.type === 'int' || field.type === 'float') {
          input = (
            <Input
              type="number"
              step={field.type === 'float' ? '0.01' : '1'}
              value={value === undefined ? '' : String(value)}
              onChange={(e) => update(field.name, parseNumber(e.target.value, field.type))}
              placeholder={placeholder}
            />
          );
        } else if (field.type === 'string_array') {
          input = (
            <Input
              value={Array.isArray(value) ? value.join(', ') : String(value || '')}
              onChange={(e) =>
                update(
                  field.name,
                  e.target.value
                    .split(',')
                    .map((s) => s.trim())
                    .filter(Boolean),
                )
              }
              placeholder={placeholder || 'value1, value2'}
            />
          );
        } else if (field.type === 'map') {
          input = (
            <Textarea
              className="min-h-24 font-mono text-xs leading-relaxed"
              value={mapText(value)}
              onChange={(e) => update(field.name, parseMapInput(e.target.value))}
              placeholder={placeholder || '{"key": "value"}'}
            />
          );
        } else {
          const multiline = ['query', 'script', 'code', 'rules', 'body'].includes(field.name);
          input = multiline ? (
            <Textarea
              className="min-h-20 font-mono text-xs leading-relaxed"
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              placeholder={placeholder}
            />
          ) : (
            <Input
              type={field.secret ? 'password' : 'text'}
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              placeholder={placeholder}
            />
          );
        }

        return (
          <div key={field.name}>
            <Label className="mb-1.5 flex items-center gap-1 text-xs text-muted-foreground">
              <span>{field.name}</span>
              {field.required && <span className="text-rose-500">*</span>}
              {field.secret && (
                <ToneBadge tone="amber" className="px-1.5 py-0 text-[10px]">
                  {t('ui.secret')}
                </ToneBadge>
              )}
              {field.scope === 'behavior' && (
                <ToneBadge tone="slate" className="px-1.5 py-0 text-[10px]">
                  {t('field.scopeBehavior')}
                </ToneBadge>
              )}
              {field.scope === 'connection' && (
                <ToneBadge tone="blue" className="px-1.5 py-0 text-[10px]">
                  {t('field.scopeConnection')}
                </ToneBadge>
              )}
              {field.default !== undefined && (
                <span className="ml-auto text-[10px] text-muted-foreground/80">
                  {t('conn.default')}: {exampleText(field.default)}
                </span>
              )}
            </Label>
            {input}
            {field.description && (
              <div className={cn('mt-1 text-xs text-muted-foreground')}>{field.description}</div>
            )}
          </div>
        );
      })}
    </div>
  );
}
