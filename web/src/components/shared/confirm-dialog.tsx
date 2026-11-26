import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { getLang, translate } from '@/i18n';
import type { ReactNode } from 'react';

export function ConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel,
  cancelLabel,
  destructive,
  onConfirm,
  details,
  confirmDisabled,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  onConfirm: () => void;
  details?: ReactNode;
  confirmDisabled?: boolean;
}) {
  // UI-D.1: fallbacks are localized (a bare 'OK'/'Cancel' leaked English in
  // ZH dialogs); callers may still override either label.
  const resolvedConfirm = confirmLabel ?? translate('ui.confirm', getLang());
  const resolvedCancel = cancelLabel ?? translate('ui.cancel', getLang());
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md" data-testid="confirm-dialog">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          {description ? <DialogDescription>{description}</DialogDescription> : null}
        </DialogHeader>
        {details}
        <DialogFooter>
          <Button
            variant="outline"
            data-testid="confirm-dialog-cancel"
            onClick={() => onOpenChange(false)}
          >
            {resolvedCancel}
          </Button>
          <Button
            variant={destructive ? 'destructive' : 'default'}
            data-testid="confirm-dialog-confirm"
            disabled={confirmDisabled}
            onClick={() => {
              onOpenChange(false);
              onConfirm();
            }}
          >
            {resolvedConfirm}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
