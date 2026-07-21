import { useEffect, useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { api } from '@/lib/api';
import type { TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';
import { showToast } from '@/lib/toast';

type Props = {
  t: TFunc;
  lang: Lang;
  token: string;
  setToken: (v: string) => void;
  switchLang: (l: Lang) => void;
  llmConfig: { base_url: string; model: string; api_key: string };
  setLLMConfig: (c: { base_url: string; model: string; api_key: string }) => void;
  open: boolean;
  onClose: () => void;
  onSaveToken: () => void;
  onSaveLLM: () => void;
};

export function SettingsModal({
  t,
  lang,
  token,
  setToken,
  switchLang,
  llmConfig,
  setLLMConfig,
  open,
  onClose,
  onSaveToken,
  onSaveLLM,
}: Props) {
  const [workerLabels, setWorkerLabels] = useState('');

  useEffect(() => {
    if (!open) return;
    api<{ labels?: Record<string, string> }>('/api/v2/workers/standalone-worker')
      .then((d: any) => {
        if (d?.labels) {
          setWorkerLabels(
            Object.entries(d.labels)
              .map(([k, v]) => `${k}=${v}`)
              .join(', '),
          );
        }
      })
      .catch(() => {});
  }, [open]);

  const saveWorkerLabels = () => {
    const labels: Record<string, string> = {};
    workerLabels.split(',').forEach((pair) => {
      const [k, v] = pair.trim().split('=');
      if (k && v) labels[k.trim()] = v.trim();
    });
    api('/api/v2/workers', {
      method: 'POST',
      body: JSON.stringify({
        id: 'standalone-worker',
        host: 'localhost',
        port: 0,
        slots: 4,
        labels,
      }),
    })
      .then(() => showToast('success', t('settings.workerLabelsSaved')))
      .catch((e) => showToast('error', e.message));
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-h-[85vh] gap-0 overflow-hidden p-0 sm:max-w-2xl">
        <DialogHeader className="border-b border-border px-6 py-4">
          <DialogTitle>{t('nav.settings')}</DialogTitle>
        </DialogHeader>

        <Tabs defaultValue="general" className="flex flex-col">
          <div className="border-b border-border px-6">
            <TabsList className="h-auto w-full justify-start gap-1 rounded-none bg-transparent p-0">
              <TabsTrigger
                value="general"
                className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none"
              >
                {t('settings.tabGeneral')}
              </TabsTrigger>
              <TabsTrigger
                value="llm"
                className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none"
              >
                {t('settings.tabLLM')}
              </TabsTrigger>
              <TabsTrigger
                value="worker"
                className="rounded-none border-b-2 border-transparent data-[state=active]:border-primary data-[state=active]:bg-transparent data-[state=active]:shadow-none"
              >
                {t('settings.tabWorker')}
              </TabsTrigger>
            </TabsList>
          </div>

          <div className="max-h-[60vh] overflow-y-auto px-6 py-5">
            <TabsContent value="general" className="mt-0 space-y-5">
              <div className="space-y-2">
                <Label>{t('settings.language')}</Label>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    variant={lang === 'en' ? 'default' : 'secondary'}
                    className="flex-1"
                    onClick={() => switchLang('en')}
                  >
                    English
                  </Button>
                  <Button
                    size="sm"
                    variant={lang === 'zh' ? 'default' : 'secondary'}
                    className="flex-1"
                    onClick={() => switchLang('zh')}
                  >
                    中文
                  </Button>
                </div>
              </div>
              <div className="space-y-2">
                <Label>{t('settings.apiToken')}</Label>
                <Input
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  placeholder={t('settings.tokenPlaceholder')}
                />
                <Button variant="secondary" size="sm" onClick={onSaveToken}>
                  {t('settings.saveToken')}
                </Button>
              </div>
              <div className="rounded-lg border border-border bg-muted/40 px-4 py-3 text-xs text-muted-foreground">
                💡 {t('settings.runtimeHint')}
              </div>
            </TabsContent>

            <TabsContent value="llm" className="mt-0 space-y-4">
              <p className="text-sm text-muted-foreground">{t('settings.llmDesc')}</p>
              <div className="space-y-2">
                <Label>{t('settings.llmBaseUrl')}</Label>
                <Input
                  value={llmConfig.base_url}
                  onChange={(e) => setLLMConfig({ ...llmConfig, base_url: e.target.value })}
                  placeholder="https://api.openai.com/v1"
                />
              </div>
              <div className="space-y-2">
                <Label>{t('settings.llmModel')}</Label>
                <Input
                  value={llmConfig.model}
                  onChange={(e) => setLLMConfig({ ...llmConfig, model: e.target.value })}
                  placeholder="gpt-4o"
                />
              </div>
              <div className="space-y-2">
                <Label>{t('settings.llmApiKey')}</Label>
                <Input
                  type="password"
                  value={llmConfig.api_key}
                  onChange={(e) => setLLMConfig({ ...llmConfig, api_key: e.target.value })}
                  placeholder="sk-..."
                />
              </div>
              <div className="rounded-lg bg-primary/5 px-4 py-3 text-xs text-primary">
                💡 {t('settings.llmHint')}
              </div>
              <Button onClick={onSaveLLM}>{t('settings.llmSave')}</Button>
            </TabsContent>

            <TabsContent value="worker" className="mt-0 space-y-4">
              <p className="text-sm text-muted-foreground">{t('settings.workerDesc')}</p>
              <div className="space-y-2">
                <Label>{t('settings.workerLabels')}</Label>
                <Input
                  value={workerLabels}
                  onChange={(e) => setWorkerLabels(e.target.value)}
                  placeholder="zone=us-east, gpu=true, highmem=true"
                />
                <p className="text-xs text-muted-foreground">{t('settings.workerLabelsHint')}</p>
              </div>
              <Button onClick={saveWorkerLabels}>{t('settings.workerLabelsSave')}</Button>
            </TabsContent>
          </div>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
