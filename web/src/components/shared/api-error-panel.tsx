import { ErrorDetails } from '@/components/shared/error-details';
import type { ApiIssue } from '@/lib/api';

export type ApiErrorPanelProps = {
  error: unknown;
  title?: string;
  onDismiss?: () => void;
  onNavigate?: (issue: ApiIssue) => void;
  navigateLabel?: string;
  className?: string;
  testId?: string;
};

export function ApiErrorPanel({
  error,
  title = 'Action needs attention',
  onDismiss,
  onNavigate,
  navigateLabel = 'Go to fix',
  className = '',
  testId = 'api-error-panel',
}: ApiErrorPanelProps) {
  return (
    <ErrorDetails
      error={error}
      title={title}
      className={className}
      testId={testId}
      onDismiss={onDismiss}
      onNavigate={onNavigate}
      navigateLabel={navigateLabel}
    />
  );
}
