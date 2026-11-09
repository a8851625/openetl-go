import React, { useState } from 'react';
import { Eye, EyeOff } from 'lucide-react';
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

export type ConfigFieldIssue = {
  level?: string;
  check?: string;
  message: string;
  remediation?: string;
};

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

/**
 * Prefer a short form for first-run UIs: required fields, or fields whose value is still empty.
 * Optional filled fields stay hidden until the caller expands "more options".
 */
export function essentialFields(
  fields: PluginSchemaField[] = [],
  config: Record<string, unknown> = {},
  opts?: { maxOptionalEmpty?: number },
) {
  const maxOptionalEmpty = opts?.maxOptionalEmpty ?? 3;
  const required = fields.filter((f) => f.required);
  const optionalEmpty = fields.filter((f) => {
    if (f.required) return false;
    const value = config[f.name];
    if (value === undefined || value === null) return true;
    if (typeof value === 'string') return value.trim() === '';
    if (Array.isArray(value)) return value.length === 0;
    return false;
  });
  // Keep form short: required + a few empty optional knobs (table/topic/etc.).
  const picked = [...required];
  for (const f of optionalEmpty) {
    if (picked.length >= required.length + maxOptionalEmpty) break;
    // Prefer common "which object" fields over long-tail options.
    if (
      /^(table|tables|topic|topics|database|db|bucket|path|prefix|query|object|index)$/i.test(f.name) ||
      f.name.endsWith('_table') ||
      f.name.endsWith('_topic')
    ) {
      picked.push(f);
    }
  }
  // If still short, add remaining empty optionals until cap.
  for (const f of optionalEmpty) {
    if (picked.some((p) => p.name === f.name)) continue;
    if (picked.length >= required.length + maxOptionalEmpty) break;
    picked.push(f);
  }
  // Preserve original schema order.
  const order = new Map(fields.map((f, i) => [f.name, i]));
  return picked.sort((a, b) => (order.get(a.name) ?? 0) - (order.get(b.name) ?? 0));
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
  fieldIssues = {},
  fieldPathPrefix,
}: {
  fields: PluginSchemaField[];
  config: Record<string, unknown>;
  onChange: (cfg: Record<string, unknown>) => void;
  t: TFunc;
  emptyText?: string;
  fieldIssues?: Record<string, ConfigFieldIssue[]>;
  fieldPathPrefix?: string;
}) {
  // Hooks must run in a stable order on every render: declare them before the
  // empty-fields early return, otherwise a config that goes from "no fields"
  // to "has fields" renders more hooks than the previous pass and React throws.
  const [touched, setTouched] = useState<Record<string, boolean>>({});
  const [revealed, setRevealed] = useState<Record<string, boolean>>({});

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

  // Per-field local validation state. The server preflight only runs on
  // submit, so until now an empty required field produced no feedback while
  // the operator was typing. Fields are marked "touched" on blur and only then
  // does the required check speak up (never scold a field the user has not
  // visited yet).
  const requiredMissing = (field: PluginSchemaField, value: unknown) => {
    if (!field.required) return false;
    if (value === undefined || value === null) return true;
    if (typeof value === 'string') return value.trim() === '';
    if (Array.isArray(value)) return value.length === 0;
    return false;
  };

  // Map fields hold a raw JSON document; once the user types something that
  // does not parse we keep their text on screen and flag it instead of
  // silently swallowing the edit.
  const mapInvalid = (field: PluginSchemaField, value: unknown) => {
    if (field.type !== 'map') return false;
    if (typeof value !== 'string' || value.trim() === '') return false;
    try {
      const parsed = JSON.parse(value);
      return !(parsed && typeof parsed === 'object' && !Array.isArray(parsed));
    } catch {
      return true;
    }
  };

  // Long connector schemas (mysql_cdc exposes 16+) read as an undifferentiated
  // wall. Split them into the two scopes the backend already declares, with the
  // connection block collapsed once it is complete.
  const grouped = fields.length > 8;
  const scopeGroups = grouped
    ? [
        { key: 'connection', label: t('field.groupConnection'), items: fields.filter((f) => f.scope === 'connection') },
        { key: 'behavior', label: t('field.groupBehavior'), items: fields.filter((f) => f.scope !== 'connection') },
      ].filter((g) => g.items.length > 0)
    : [{ key: 'all', label: '', items: fields }];

  const renderField = (field: PluginSchemaField) => {
        const value = config[field.name] ?? field.default ?? '';
        const placeholder = exampleText(field.example) || field.description || '';
        const issues = fieldIssues?.[field.name] || fieldIssues?.[`${fieldPathPrefix || ''}.${field.name}`] || [];
        const fieldPath = fieldPathPrefix ? `${fieldPathPrefix}.${field.name}` : field.name;
        const hasError = issues.some((issue) => issue.level !== 'warning' && issue.level !== 'info');
        const fieldId = `cfg-${fieldPathPrefix ? fieldPathPrefix.replace(/[^a-z0-9]+/gi, '-') + '-' : ''}${field.name}`;
        const descId = `${fieldId}-desc`;
        const errId = `${fieldId}-err`;
        const isTouched = touched[field.name] === true;
        const missingRequired = requiredMissing(field, value);
        const jsonBroken = mapInvalid(field, value);
        // A field only shows the required/JSON error once the operator has left
        // it (or the server already reported a problem), so an untouched form
        // does not open covered in red.
        const localInvalid = (isTouched && missingRequired) || jsonBroken;
        const showInvalid = hasError || localInvalid;
        // Wire descriptions and messages to the control so screen readers
        // announce them with the field instead of leaving them orphaned text.
        const describedBy =
          [field.description ? descId : null, showInvalid ? errId : null].filter(Boolean).join(' ') || undefined;
        // Credential-ish fields get a real autocomplete token so password
        // managers and browser autofill work instead of guessing.
        const autoCompleteFor = (name: string): string | undefined => {
          if (/^(user|username|user_name)$/i.test(name)) return 'username';
          if (/password|passwd|secret/i.test(name)) return 'current-password';
          if (/^(url|endpoint|host|hostname)$/i.test(name)) return 'url';
          return undefined;
        };
        // Semantic label: i18n field.<name> (falls back to the raw name when
        // no key exists); the raw field name stays visible on hover so the
        // docs/JSON mapping is never hidden.
        // Display name: an explicit translation when one exists, otherwise a
        // readable "Field Path" rather than the raw "field.path" key. The raw
        // key stays in the label's title attribute for docs/JSON mapping.
        const translated = t(`field.${field.name}`);
        const fieldLabel =
          translated && translated !== `field.${field.name}`
            ? translated
            : field.name
                .replace(/[_-]+/g, ' ')
                .replace(/\b\w/g, (c) => c.toUpperCase());
        const invalidClass = hasError ? 'border-rose-400 ring-1 ring-rose-200' : '';
        let input: React.ReactNode;
        if (field.enum && field.enum.length > 0) {
          input = (
            <select
              id={fieldId}
              className={cn(selectClass, invalidClass)}
              aria-invalid={showInvalid}
              aria-describedby={describedBy}
              value={String(value)}
              onChange={(e) => update(field.name, e.target.value)}
              onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
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
            <div className={cn('flex h-9 items-center gap-2 rounded-md border border-transparent px-2', invalidClass)}>
              <Switch
                id={fieldId}
                aria-invalid={showInvalid}
                aria-describedby={describedBy}
                checked={!!value}
                onCheckedChange={(checked) => update(field.name, checked)}
              />
              <span className="text-xs text-muted-foreground">
                {value ? t('common.enabled') : t('common.disabled')}
              </span>
            </div>
          );
        } else if (field.type === 'int' || field.type === 'float') {
          input = (
            <Input
              id={fieldId}
              className={invalidClass}
              aria-invalid={showInvalid}
              aria-describedby={describedBy}
              onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
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
              id={fieldId}
              className={invalidClass}
              aria-invalid={showInvalid}
              aria-describedby={describedBy}
              onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
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
              id={fieldId}
              className={cn('min-h-24 font-mono text-xs leading-relaxed', jsonBroken ? 'border-rose-400 ring-1 ring-rose-200' : invalidClass)}
              aria-invalid={showInvalid}
              aria-describedby={describedBy}
              // Keep the raw text while it is unparseable so the operator can
              // fix a typo instead of losing the whole document.
              value={jsonBroken ? String(value) : mapText(value)}
              onChange={(e) => {
                const raw = e.target.value;
                const parsed = parseMapInput(raw);
                update(field.name, typeof parsed === 'string' ? raw : parsed);
              }}
              onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
              placeholder={placeholder || '{"key": "value"}'}
            />
          );
        } else {
          const multiline = ['query', 'script', 'code', 'rules', 'body'].includes(field.name);
          input = multiline ? (
            <Textarea
              id={fieldId}
              className={cn('min-h-20 font-mono text-xs leading-relaxed', invalidClass)}
              aria-invalid={issues.length > 0}
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              placeholder={placeholder}
            />
          ) : field.secret ? (
            <div className="relative">
              <Input
                id={fieldId}
                className={cn(invalidClass, 'pr-10')}
                aria-invalid={showInvalid}
                aria-describedby={describedBy}
                autoComplete={autoCompleteFor(field.name)}
                type={revealed[field.name] ? 'text' : 'password'}
                value={String(value || '')}
                onChange={(e) => update(field.name, e.target.value)}
                onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
                placeholder={placeholder}
              />
              {/* Secrets are typed blind; without a reveal affordance a long
                  password cannot be verified before saving. */}
              <button
                type="button"
                className="absolute inset-y-0 right-0 flex w-9 items-center justify-center text-muted-foreground hover:text-foreground"
                aria-label={revealed[field.name] ? t('field.hideSecret') : t('field.showSecret')}
                aria-pressed={!!revealed[field.name]}
                onClick={() => setRevealed((cur) => ({ ...cur, [field.name]: !cur[field.name] }))}
              >
                {revealed[field.name] ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
              </button>
            </div>
          ) : (
            <Input
              id={fieldId}
              className={invalidClass}
              aria-invalid={showInvalid}
              aria-describedby={describedBy}
              autoComplete={autoCompleteFor(field.name)}
              type="text"
              value={String(value || '')}
              onChange={(e) => update(field.name, e.target.value)}
              onBlur={() => setTouched((cur) => ({ ...cur, [field.name]: true }))}
              placeholder={placeholder}
            />
          );
        }

        return (
          <div
            key={field.name}
            data-testid={`config-field-${field.name}`}
            data-field-path={fieldPath}
            tabIndex={-1}
            className={cn(
              'rounded-md p-1 -m-1',
              showInvalid && 'border border-rose-300 bg-rose-50/60 dark:border-rose-800 dark:bg-rose-950/20',
              issues.length > 0 && !hasError && 'border border-amber-300 bg-amber-50/60 dark:border-amber-800 dark:bg-amber-950/20',
            )}
          >
            <Label htmlFor={fieldId} title={field.name} className="mb-1.5 flex items-center gap-1 text-xs text-muted-foreground">
              <span>{fieldLabel}</span>
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
              {/* The input is already seeded with field.default, so printing
                  "Default: <same value>" next to it said nothing. Only surface
                  the default once the operator has changed it away from it,
                  where it becomes a useful "reset to" hint. */}
              {field.default !== undefined &&
                String(value) !== String(field.default) && (
                  <span className="ml-auto text-[10px] text-muted-foreground/80">
                    {t('conn.default')}: {exampleText(field.default)}
                  </span>
                )}
            </Label>
            <div aria-invalid={hasError || undefined}>{input}</div>
            {field.description && (
              <div id={descId} className={cn('mt-1 text-xs text-muted-foreground')}>
                {field.description}
              </div>
            )}
            {/* Local checks speak only after the field has been visited; they
                use the same id as server issues so aria-describedby is stable. */}
            {localInvalid && !hasError && (
              <div
                id={errId}
                className="mt-1 rounded border border-rose-200 bg-rose-50 px-2 py-1 text-[11px] text-rose-700 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-300"
                data-testid={`config-field-${field.name}-local-error`}
              >
                {jsonBroken ? t('field.invalidJson') : t('field.requiredMissing')}
              </div>
            )}
            {issues.map((issue, index) => (
              <div
                key={`${issue.check || 'issue'}-${index}`}
                id={index === 0 ? errId : undefined}
                className={cn(
                  'mt-1 rounded border px-2 py-1 text-[11px]',
                  issue.level === 'warning' || issue.level === 'info'
                    ? 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-300'
                    : 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900 dark:bg-rose-950/20 dark:text-rose-300',
                )}
                data-testid={`config-field-${field.name}-error`}
              >
                <div className="font-medium">{issue.check || issue.level || 'Invalid value'}</div>
                <div className="break-words whitespace-pre-wrap">{issue.message}</div>
                {issue.remediation && <div className="mt-0.5 break-words whitespace-pre-wrap opacity-80">Fix: {issue.remediation}</div>}
              </div>
            ))}
          </div>
        );
  };

  return (
    <div className="space-y-3">
      {grouped && (
        <div className="flex flex-wrap gap-2 rounded-md border border-border bg-muted/30 px-2 py-1.5 text-[10px] text-muted-foreground">
          <span className="font-medium">{t('field.legendTitle')}</span>
          <span className="inline-flex items-center gap-1">
            <span className="rounded bg-blue-100 px-1 text-blue-700 dark:bg-blue-950 dark:text-blue-300">{t('field.scopeConnection')}</span>
            {t('field.legendConnection')}
          </span>
          <span className="inline-flex items-center gap-1">
            <span className="rounded bg-slate-200 px-1 text-slate-700 dark:bg-slate-800 dark:text-slate-300">{t('field.scopeBehavior')}</span>
            {t('field.legendBehavior')}
          </span>
        </div>
      )}
      {scopeGroups.map((group) => (
        <div key={group.key} className="space-y-3">
          {group.label && (
            <div className="border-b border-border pb-1 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              {group.label}
            </div>
          )}
          {group.items.map((field) => renderField(field))}
        </div>
      ))}
    </div>
  );
}
