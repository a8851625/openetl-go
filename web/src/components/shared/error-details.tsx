import { useMemo, useState } from 'react';
import { ArrowRight, Check, ChevronDown, ChevronUp, Copy, TriangleAlert, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { toApiErrorDetails, type ApiErrorDetails, type ApiIssue } from '@/lib/api';

type ErrorDetailsProps = {
  error: unknown;
  title?: string;
  tone?: 'error' | 'warning' | 'info';
  className?: string;
  testId?: string;
  onDismiss?: () => void;
  onNavigate?: (issue: ApiIssue) => void;
  navigateLabel?: string;
};

function issueLabel(issue: ApiErrorDetails['issues'][number]) {
  return [issue.level, issue.node_id || issue.node, displayField(issue.field), issue.check || issue.code]
    .filter(Boolean)
    .join(' · ');
}

function displayField(field?: string) {
  if (!field) return '';
  const dagPath = field.match(/^dag\.nodes\.([^\.]+)(.*)$/);
  return dagPath ? `dag.nodes[${dagPath[1]}]${dagPath[2]}` : field;
}

export function ErrorDetails({
  error,
  title = 'Configuration failed',
  tone = 'error',
  className,
  testId = 'error-details',
  onDismiss,
  onNavigate,
  navigateLabel = 'Open field',
}: ErrorDetailsProps) {
  const details = useMemo(() => toApiErrorDetails(error), [error]);
  const [rawOpen, setRawOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  if (!error) return null;

  const palette = tone === 'warning'
    ? {
        shell: 'border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-200',
        card: 'border-amber-200/80 bg-white/70 dark:border-amber-900/70 dark:bg-amber-950/20',
      }
    : tone === 'info'
      ? {
          shell: 'border-sky-200 bg-sky-50 text-sky-900 dark:border-sky-900 dark:bg-sky-950/30 dark:text-sky-200',
          card: 'border-sky-200/80 bg-white/70 dark:border-sky-900/70 dark:bg-sky-950/20',
        }
      : {
          shell: 'border-rose-200 bg-rose-50 text-rose-900 dark:border-rose-900 dark:bg-rose-950/30 dark:text-rose-200',
          card: 'border-rose-200/80 bg-white/70 dark:border-rose-900/70 dark:bg-rose-950/20',
        };

  const copyDetails = async () => {
    const lines = [details.summary];
    details.issues.forEach((issue) => {
      const prefix = [issue.node_id || issue.node, displayField(issue.field), issue.check || issue.code].filter(Boolean).join(' · ');
      lines.push(`${prefix ? `${prefix}: ` : ''}${issue.message}`);
      if (issue.remediation) lines.push(`Fix: ${issue.remediation}`);
      if (issue.action) lines.push(`Action: ${issue.action}`);
    });
    if (details.raw) lines.push(`Technical details:\n${details.raw}`);
    try {
      await navigator.clipboard.writeText(lines.join('\n'));
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div
      role="alert"
      aria-live="polite"
      data-testid={testId}
      className={cn('rounded-lg border px-3 py-2.5 text-sm', palette.shell, className)}
    >
      <div className="flex items-start gap-2">
        <TriangleAlert className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div className="font-semibold">{title}</div>
            <div className="flex shrink-0 items-center gap-1">
              {details.status ? <span className="text-[10px] opacity-70">HTTP {details.status}</span> : null}
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                title={copied ? 'Copied' : 'Copy error details'}
                aria-label={copied ? 'Copied' : 'Copy error details'}
                onClick={copyDetails}
              >
                {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
              </Button>
              {onDismiss && (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7"
                  title="Dismiss error details"
                  aria-label="Dismiss error details"
                  onClick={onDismiss}
                >
                  <X className="h-3.5 w-3.5" />
                </Button>
              )}
            </div>
          </div>
          <div className="mt-1 break-words whitespace-pre-wrap">{details.summary}</div>
          {details.issues.length > 0 && (
            <div className="mt-2 max-h-[min(52vh,520px)] space-y-2 overflow-y-auto pr-1" data-testid={`${testId}-issues`}>
              {details.issues.map((issue, index) => (
                <div
                  key={`${issueKey(issue)}-${index}`}
                  className={cn('rounded border p-2 text-xs', palette.card)}
                  data-testid={`${testId}-issue-${index}`}
                >
                  {issueLabel(issue) && <div className="font-semibold">{issueLabel(issue)}</div>}
                  <div className="mt-0.5 break-words whitespace-pre-wrap">{issue.message}</div>
                  {issue.remediation && <div className="mt-1 break-words whitespace-pre-wrap text-muted-foreground">Fix: {issue.remediation}</div>}
                  {issue.action && <div className="mt-1 break-words whitespace-pre-wrap text-muted-foreground">Action: {issue.action}</div>}
                  {onNavigate && (issue.field || issue.node_id || issue.node) && (
                    <Button
                      type="button"
                      variant="link"
                      size="sm"
                      className="mt-1 h-auto px-0 py-0 text-xs"
                      onClick={() => onNavigate(issue)}
                      data-testid={`${testId}-issue-${index}-navigate`}
                    >
                      {navigateLabel} <ArrowRight className="ml-1 h-3 w-3" />
                    </Button>
                  )}
                </div>
              ))}
            </div>
          )}
          {details.raw && (
            <div className="mt-2">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-7 px-1.5 text-[11px]"
                onClick={() => setRawOpen((open) => !open)}
                aria-expanded={rawOpen}
              >
                {rawOpen ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
                Technical details
              </Button>
              {rawOpen && <pre className="mt-1 max-h-56 overflow-auto whitespace-pre-wrap break-words rounded border border-border/60 bg-black/5 p-2 font-mono text-[11px]">{details.raw}</pre>}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function issueKey(issue: ApiErrorDetails['issues'][number]) {
  return [issue.level, issue.field, issue.node_id || issue.node, issue.check, issue.code, issue.message].join('|');
}
