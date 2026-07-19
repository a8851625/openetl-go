import React, { useState, useRef, useCallback, useEffect } from 'react';
import type { TFunc, Lang } from './types';
import Editor from '@monaco-editor/react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import { showToast as notifyToast } from '@/lib/toast';
import { cn } from '@/lib/utils';
import { useTheme } from '@/components/theme-provider';

const selectClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring';

type InstalledPlugin = {
  name: string;
  kind: string;
  wasm_path: string;
  version: string;
  abi?: string;
  min_runtime_version?: string;
  manifest_validated?: boolean;
  manifest?: any;
  enabled: boolean;
  installed_at: string;
};

type PluginKind = 'transform' | 'source' | 'sink';

type Tab = 'installed' | 'editor';

// ── Templates ──────────────────────────────────────────────────────────

const TEMPLATES: Record<PluginKind, string> = {
  transform: `import { createExtismTransformPlugin } from '@etl/sdk';

interface MyConfig {
  prefix?: string;
}

const plugin = createExtismTransformPlugin({
  name: 'my-transform',
  version: '1.0.0',
  apply(record, ctx) {
    const cfg = ctx.config as MyConfig;
    ctx.log(\`Processing \${record.operation} from \${record.metadata.source}\`);
    record.data['processed_at'] = new Date().toISOString();
    if (cfg.prefix) {
      record.data['prefixed_id'] = cfg.prefix + '_' + (record.data['id'] ?? '');
    }
    return record;
  },
});

export const transform = plugin;
`,
  source: `import { createExtismSourcePlugin } from '@etl/sdk';

const plugin = createExtismSourcePlugin({
  name: 'my-source',
  version: '1.0.0',
  open(ctx) {
    ctx.log('Source opened');
  },
  read(ctx) {
    // Return null when exhausted
    return {
      operation: 'INSERT',
      data: { message: 'hello', ts: Date.now() },
      metadata: {
        source: 'my-source',
        timestamp: new Date().toISOString(),
      },
    };
  },
  close(ctx) {
    ctx.log('Source closed');
  },
});

export const read = plugin;
`,
  sink: `import { createExtismSinkPlugin } from '@etl/sdk';

const plugin = createExtismSinkPlugin({
  name: 'my-sink',
  version: '1.0.0',
  open(ctx) {
    ctx.log('Sink opened');
  },
  write(records, ctx) {
    ctx.log(\`Writing \${records.length} records\`);
    for (const rec of records) {
      ctx.log(\`  \${rec.operation}: \${JSON.stringify(rec.data)}\`);
    }
  },
  close(ctx) {
    ctx.log('Sink closed');
  },
});

export const write = plugin;
`,
};

const KIND_OPTIONS: PluginKind[] = ['transform', 'source', 'sink'];
const OPENETL_PLUGIN_ABI = 'openetl.plugin.abi/v1';
const OPENETL_MIN_RUNTIME_VERSION = 'openetl-runtime/v1';
const ENTRYPOINT_BY_KIND: Record<PluginKind, string> = {
  transform: 'transform',
  source: 'read',
  sink: 'write',
};

function buildPluginManifest(name: string, kind: PluginKind, version: string) {
  return {
    name,
    kind,
    version: version || '1.0.0',
    abi: OPENETL_PLUGIN_ABI,
    min_runtime_version: OPENETL_MIN_RUNTIME_VERSION,
    entrypoints: [ENTRYPOINT_BY_KIND[kind]],
  };
}

// Real-world TS plugin example for the editor
const VIP_EXAMPLE_SOURCE = `/**
 * VIP Order Enricher — A real-world TypeScript transform plugin.
 * Compiles to WASM via extism-js, then installed as plugin_vip-order-enricher.
 *
 * Features: adds timestamp, classifies order tier, masks sensitive fields,
 * computes risk score, logs high-risk orders.
 */
import { createExtismTransformPlugin } from '@etl/sdk';

const plugin = createExtismTransformPlugin({
  name: 'vip-order-enricher',
  version: '1.0.0',
  apply(record, ctx) {
    const cfg = ctx.config as { vip_threshold?: number; mask_fields?: string[] };
    const threshold = cfg.vip_threshold ?? 10000;
    const data = record.data as Record<string, any>;

    // 1. Add processing metadata
    data.processed_at = new Date().toISOString();

    // 2. Classify order tier
    const amount = Number(data.amount) || 0;
    data.order_tier = amount >= threshold ? 'vip' : amount >= threshold * 0.5 ? 'premium' : 'standard';

    // 3. Mask sensitive fields
    const maskFields = cfg.mask_fields ?? ['credit_card', 'ssn'];
    for (const field of maskFields) {
      if (data[field]) {
        const val = String(data[field]);
        data[field] = val.length > 4 ? '*'.repeat(val.length - 4) + val.slice(-4) : '****';
      }
    }

    // 4. Compute risk score
    let risk = data.order_tier === 'vip' ? 40 : data.order_tier === 'premium' ? 20 : 10;
    if (data.status === 'pending') risk += 30;
    if (amount > 50000) risk += 20;
    data.risk_score = Math.min(100, risk);

    if (risk >= 70) ctx.log('HIGH RISK order: ' + data.order_id);

    return record;
  },
});

export const transform = plugin;
`;

const DEFAULT_SAMPLE: Record<string, any> = {
  operation: 'INSERT',
  data: { id: 1, name: 'test', email: 'test@example.com' },
  metadata: {
    source: 'test',
    table: 'users',
    timestamp: new Date().toISOString(),
  },
};

// ── API ─────────────────────────────────────────────────────────────────

function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  if (!headers.has('Content-Type') && init.body && typeof init.body === 'string') headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status}`);
  return res.json();
}

// ── Component ────────────────────────────────────────────────────────────

export function MyPluginsPage({ t, lang: _lang }: { t: TFunc; lang: Lang }) {
  const { resolvedTheme } = useTheme();
  const [tab, setTab] = useState<Tab>('installed');
  const [plugins, setPlugins] = useState<InstalledPlugin[]>([]);
  const [loading, setLoading] = useState(true);
  // Editor state
  const [editorKind, setEditorKind] = useState<PluginKind>('transform');
  const [editorName, setEditorName] = useState('my-transform');
  const [editorVersion, setEditorVersion] = useState('1.0.0');
  const [editorSource, setEditorSource] = useState(TEMPLATES.transform);
  const [isCompiling, setIsCompiling] = useState(false);

  // Debug state
  const [sampleRecord, setSampleRecord] = useState(JSON.stringify(DEFAULT_SAMPLE, null, 2));
  const [debugRunning, setDebugRunning] = useState(false);
  const [debugOutput, setDebugOutput] = useState<any>(null);
  const [debugError, setDebugError] = useState('');

  // Upload state
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploadName, setUploadName] = useState('');
  const [uploadKind, setUploadKind] = useState<PluginKind>('transform');
  const [uploadVersion, setUploadVersion] = useState('1.0.0');
  const fileRef = useRef<HTMLInputElement>(null);

  const showToast = useCallback((msg: string) => { notifyToast('info', msg); }, []);
  const editorManifest = JSON.stringify(buildPluginManifest(editorName.trim(), editorKind, editorVersion.trim()), null, 2);

  const refresh = useCallback(() => {
    setLoading(true);
    api<{ installed?: InstalledPlugin[] }>('/api/v2/plugins')
      .then((d) => setPlugins(d.installed || []))
      .catch(() => setPlugins([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  // ── Tab switching ──

  const switchToEditor = useCallback((source?: string, name?: string, kind?: PluginKind, version?: string) => {
    setEditorSource(source ?? TEMPLATES.transform);
    setEditorName(name ?? 'my-transform');
    setEditorKind(kind ?? 'transform');
    setEditorVersion(version ?? '1.0.0');
    setDebugOutput(null);
    setDebugError('');
    setTab('editor');
  }, []);

  const switchToInstalled = useCallback(() => {
    setTab('installed');
    refresh();
  }, [refresh]);

  // ── Kind change ──

  const handleKindChange = useCallback((kind: PluginKind) => {
    // Only swap template if the editor source matches the old template
    setEditorKind(kind);
    // Don't overwrite user's code - they can click "Load Template" if they want
  }, []);

  const loadTemplate = useCallback(() => {
    setEditorSource(TEMPLATES[editorKind]);
    setEditorName(`my-${editorKind}`);
    setEditorVersion('1.0.0');
  }, [editorKind]);

  // Load the VIP order enricher example (a real-world plugin)
  const loadExample = useCallback((_kind: string) => {
    setEditorSource(VIP_EXAMPLE_SOURCE);
    setEditorName('vip-order-enricher');
    setEditorKind('transform');
    setEditorVersion('1.0.0');
  }, []);

  // ── Download TS Source ──

  const downloadSource = useCallback((name: string, source: string) => {
    const blob = new Blob([source], { type: 'text/typescript' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${name}.ts`;
    a.click();
    URL.revokeObjectURL(url);
  }, []);

  // ── Compile ──

  const handleCompile = useCallback(async () => {
    if (!editorName.trim() || !editorSource.trim()) {
      showToast(t('myplugin.enterNameAndSource') || 'Please enter a plugin name and source code');
      return;
    }
    if (editorKind !== 'transform') {
      downloadSource(editorName.trim(), editorSource);
      showToast(t('myplugin.compileTransformOnly'));
      return;
    }
    setIsCompiling(true);
    try {
      const formData = new FormData();
      formData.append('source', editorSource);
      formData.append('name', editorName.trim());
      formData.append('kind', editorKind);
      formData.append('version', editorVersion.trim() || '1.0.0');
      formData.append('manifest', editorManifest);
      const token = getToken();
      const res = await fetch('/api/v2/plugins/compile', {
        method: 'POST',
        headers: token ? { 'X-API-Token': token } : {},
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      if (data.status === 'compiled_and_installed') {
        showToast(t('myplugin.compileSuccess'));
        refresh();
      } else if (data.status === 'source_only') {
        // Server couldn't compile - download the source instead
        downloadSource(editorName.trim(), editorSource);
        showToast(data.compile_output || t('myplugin.compiledButNotOnServer'));
      }
    } catch (e) {
      showToast(`${t('myplugin.compileFailed')}: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setIsCompiling(false);
    }
  }, [editorName, editorSource, editorKind, editorVersion, editorManifest, t, refresh, showToast, downloadSource]);

  // ── Upload WASM ──

  const handleUpload = useCallback(async () => {
    if (!selectedFile || !uploadName) { showToast(t('myplugin.selectFileAndName') || 'Please select a file and enter a name'); return; }
    try {
      const formData = new FormData();
      formData.append('wasm', selectedFile);
      formData.append('name', uploadName.trim());
      formData.append('kind', uploadKind);
      formData.append('version', uploadVersion.trim() || '1.0.0');
      formData.append('manifest', JSON.stringify(buildPluginManifest(uploadName.trim(), uploadKind, uploadVersion.trim()), null, 2));
      const token = getToken();
      const res = await fetch('/api/v2/plugins/install', {
        method: 'POST',
        headers: token ? { 'X-API-Token': token } : {},
        body: formData,
      });
      if (!res.ok) throw new Error(await res.text());
      showToast(`Plugin ${uploadName} installed successfully`);
      setSelectedFile(null);
      setUploadName('');
      setUploadVersion('1.0.0');
      if (fileRef.current) fileRef.current.value = '';
      refresh();
    } catch (e) {
      showToast(`Install failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, [selectedFile, uploadName, uploadKind, uploadVersion, showToast, refresh]);

  // ── Debug Run ──

  const handleDebug = useCallback(async () => {
    if (!editorName.trim()) { showToast(t('myplugin.enterPluginName') || 'Please enter a plugin name'); return; }
    let record: any;
    try {
      record = JSON.parse(sampleRecord);
    } catch {
      setDebugError('Invalid JSON in sample record');
      return;
    }
    setDebugRunning(true);
    setDebugError('');
    setDebugOutput(null);
    try {
      const data = await api<any>('/api/v2/plugins/dry-run', {
        method: 'POST',
        body: JSON.stringify({ name: editorName.trim(), record }),
      });
      if (data.error) {
        setDebugError(data.error);
      } else {
        setDebugOutput(data);
      }
    } catch (e) {
      setDebugError(e instanceof Error ? e.message : String(e));
    } finally {
      setDebugRunning(false);
    }
  }, [editorName, sampleRecord, showToast]);

  // ── Uninstall ──

  const uninstall = useCallback(async (name: string) => {
    try {
      await api(`/api/v2/plugins/${name}`, { method: 'DELETE' });
      showToast(`Plugin ${name} uninstalled`);
      refresh();
    } catch (e) {
      showToast(`Uninstall failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, [showToast, refresh]);

  // ── Edit installed plugin (open in editor) ──

  const editPlugin = useCallback(async (p: InstalledPlugin) => {
    // Fetch plugin details and open in editor
    try {
      const meta = await api<any>(`/api/v2/plugins/${p.name}`);
      switchToEditor(undefined, meta.name, meta.kind as PluginKind, meta.version);
    } catch {
      switchToEditor(undefined, p.name, p.kind as PluginKind, p.version);
    }
  }, [switchToEditor]);

  // ── Monaco editor theme ──

  const beforeMount = useCallback((monaco: any) => {
    monaco.editor.defineTheme('etl-dark', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { token: 'comment', foreground: '6A9955' },
        { token: 'keyword', foreground: '569CD6' },
        { token: 'string', foreground: 'CE9178' },
        { token: 'number', foreground: 'B5CEA8' },
      ],
      colors: {
        'editor.background': '#1e293b',
        'editor.foreground': '#e2e8f0',
        'editor.lineHighlightBackground': '#334155',
        'editorCursor.foreground': '#818cf8',
        'editor.selectionBackground': '#475569',
      },
    });
  }, []);

  // ── Render ──

  const kindTone = (kind: string): 'cyan' | 'emerald' | 'violet' =>
    kind === 'source' ? 'cyan' : kind === 'sink' ? 'emerald' : 'violet';

  return (
    <div className="space-y-6">
      <Tabs value={tab} onValueChange={(v) => (v === 'installed' ? switchToInstalled() : switchToEditor())}>
        <TabsList>
          <TabsTrigger value="installed">{t('myplugin.tabInstalled')}</TabsTrigger>
          <TabsTrigger value="editor">{t('myplugin.tabEditor')}</TabsTrigger>
        </TabsList>

        <TabsContent value="installed" className="mt-4 space-y-6">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm">{t('myplugin.upload')}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginName')}</Label>
                  <Input value={uploadName} onChange={(e) => setUploadName(e.target.value)} placeholder="my-transform" />
                </div>
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginKind')}</Label>
                  <select className={selectClass} value={uploadKind} onChange={(e) => setUploadKind(e.target.value as PluginKind)}>
                    {KIND_OPTIONS.map((k) => (
                      <option key={k} value={k}>
                        {k}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginVersion')}</Label>
                  <Input value={uploadVersion} onChange={(e) => setUploadVersion(e.target.value)} />
                </div>
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.selectFile')}</Label>
                  <Input
                    ref={fileRef}
                    type="file"
                    accept=".wasm"
                    className="cursor-pointer file:mr-3 file:rounded-md file:border-0 file:bg-primary/10 file:px-3 file:py-1 file:text-xs file:font-medium file:text-primary"
                    onChange={(e) => setSelectedFile(e.target.files?.[0] || null)}
                  />
                </div>
                <div className="flex items-end">
                  <Button className="w-full" onClick={handleUpload} disabled={!selectedFile || !uploadName}>
                    {t('common.install')}
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
              <CardTitle className="text-sm">{t('myplugin.installed')}</CardTitle>
              <div className="flex items-center gap-2">
                <Button variant="secondary" size="sm" onClick={refresh}>
                  {t('dlq.refresh')}
                </Button>
                <Button size="sm" onClick={() => switchToEditor()}>
                  + {t('myplugin.newPlugin')}
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {loading ? (
                <div className="p-8 text-center text-sm text-muted-foreground">Loading...</div>
              ) : plugins.length === 0 ? (
                <div className="p-6">
                  <EmptyState text={t('myplugin.noPlugins')} hint={t('myplugin.emptyHint')} />
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t('common.name')}</TableHead>
                      <TableHead>{t('common.kind')}</TableHead>
                      <TableHead>{t('common.version')}</TableHead>
                      <TableHead>ABI</TableHead>
                      <TableHead>{t('common.status')}</TableHead>
                      <TableHead>{t('myplugin.wasmPath')}</TableHead>
                      <TableHead>{t('common.actions')}</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {plugins.map((p) => (
                      <TableRow key={p.name}>
                        <TableCell className="font-medium">{p.name}</TableCell>
                        <TableCell>
                          <ToneBadge tone={kindTone(p.kind)}>{p.kind}</ToneBadge>
                        </TableCell>
                        <TableCell className="text-sm">v{p.version}</TableCell>
                        <TableCell>
                          <div className="flex flex-col gap-1 text-xs">
                            <span className="text-muted-foreground">{p.abi || OPENETL_PLUGIN_ABI}</span>
                            <ToneBadge tone={p.manifest_validated ? 'emerald' : 'slate'}>
                              {p.manifest_validated ? 'manifest' : 'legacy'}
                            </ToneBadge>
                          </div>
                        </TableCell>
                        <TableCell>
                          <ToneBadge tone={p.enabled ? 'emerald' : 'slate'}>
                            {p.enabled ? t('common.enabled') : t('common.disabled')}
                          </ToneBadge>
                        </TableCell>
                        <TableCell className="max-w-xs truncate text-xs text-muted-foreground">{p.wasm_path}</TableCell>
                        <TableCell>
                          <div className="flex gap-1">
                            <Button variant="secondary" size="sm" onClick={() => editPlugin(p)} title={t('pipe.edit')}>
                              ✏️
                            </Button>
                            <Button variant="destructive" size="sm" onClick={() => uninstall(p.name)}>
                              {t('common.uninstall')}
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm">{t('myplugin.sdk')}</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">{t('myplugin.sdkDesc')}</p>
              <div className="mt-3 rounded-lg bg-slate-900 p-4 text-sm text-slate-300">
                <div className="text-xs text-slate-500"># Install the SDK &amp; bundler</div>
                <div className="font-mono">npm install @etl/sdk esbuild</div>
                <div className="mt-3 text-xs text-slate-500"># Install the extism-js compiler from Extism releases or your CI image</div>
                <div className="font-mono">extism-js --version</div>
                <div className="mt-3 text-xs text-slate-500"># Write your plugin (src/transform.ts)</div>
                <pre className="overflow-auto font-mono text-xs">{`import { createExtismTransformPlugin } from '@etl/sdk';

const plugin = createExtismTransformPlugin({
  name: 'add-timestamp',
  version: '1.0.0',
  apply(record, ctx) {
    ctx.log('Processing record');
    record.data['processed_at'] = new Date().toISOString();
    return record;
  }
});

export const transform = plugin;`}</pre>
                <div className="mt-3 text-xs text-slate-500"># Bundle, compile to WASM &amp; install</div>
                <div className="font-mono">esbuild src/transform.ts --bundle --platform=neutral --format=cjs --target=es2020 --outfile=dist/transform.js</div>
                <div className="font-mono">extism-js dist/transform.js -i plugin-transform.d.ts -o dist/transform.wasm</div>
                <div className="font-mono mt-1"># Then upload dist/transform.wasm in the UI</div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="editor" className="mt-4">
          <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
            <div className="space-y-4 xl:col-span-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold">{t('myplugin.editor')}</span>
                  <span className="text-xs text-muted-foreground">
                    {t('myplugin.editingFile')} {editorName}.ts
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-xs text-muted-foreground">
                    {editorKind}/{editorVersion}
                  </span>
                  <Button variant="ghost" size="sm" onClick={loadTemplate} title={t('ui.loadTemplate')}>
                    {'📄 ' + t('common.template')}
                  </Button>
                  <Button variant="ghost" size="sm" onClick={() => loadExample('vip')} title={t('ui.loadExample')}>
                    {'📦 ' + t('common.example')}
                  </Button>
                  <Button variant="secondary" size="sm" onClick={() => downloadSource(editorName, editorSource)}>
                    📥 {t('myplugin.downloadSource')}
                  </Button>
                  <Button size="sm" className={cn(isCompiling && 'opacity-50')} onClick={handleCompile} disabled={isCompiling}>
                    {isCompiling ? '⚙ ' + t('myplugin.compiling') : '⚙ ' + t('common.install')}
                  </Button>
                </div>
              </div>

              <div className="overflow-hidden rounded-xl border border-border" style={{ minHeight: '450px' }}>
                <Editor
                  height="450px"
                  defaultLanguage="typescript"
                  theme={resolvedTheme === 'dark' ? 'etl-dark' : 'vs-light'}
                  value={editorSource}
                  onChange={(val) => setEditorSource(val ?? '')}
                  beforeMount={beforeMount}
                  options={{
                    minimap: { enabled: false },
                    fontSize: 13,
                    lineNumbers: 'on',
                    scrollBeyondLastLine: false,
                    automaticLayout: true,
                    tabSize: 2,
                    wordWrap: 'on',
                    suggestOnTriggerCharacters: true,
                  }}
                />
              </div>

              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginName')}</Label>
                  <Input value={editorName} onChange={(e) => setEditorName(e.target.value)} placeholder="my-transform" />
                </div>
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginKind')}</Label>
                  <select className={selectClass} value={editorKind} onChange={(e) => handleKindChange(e.target.value as PluginKind)}>
                    {KIND_OPTIONS.map((k) => (
                      <option key={k} value={k}>
                        {k}
                      </option>
                    ))}
                  </select>
                  {editorKind !== 'transform' && (
                    <p className="mt-1 text-xs text-amber-600">{t('myplugin.compileTransformOnly')}</p>
                  )}
                </div>
                <div>
                  <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.pluginVersion')}</Label>
                  <Input value={editorVersion} onChange={(e) => setEditorVersion(e.target.value)} />
                </div>
              </div>
            </div>

            <div className="space-y-4 xl:col-span-1">
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm">{t('myplugin.debugTitle')}</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div>
                    <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.sampleRecord')}</Label>
                    <Textarea
                      className="font-mono text-xs"
                      rows={8}
                      value={sampleRecord}
                      onChange={(e) => setSampleRecord(e.target.value)}
                      placeholder={t('myplugin.pasteHere')}
                    />
                  </div>
                  <Button className="w-full" size="sm" onClick={handleDebug} disabled={debugRunning || !editorName.trim()}>
                    {debugRunning ? '...' : '▶ ' + t('myplugin.runDebug')}
                  </Button>
                  <div>
                    <Label className="mb-1.5 text-xs text-muted-foreground">{t('myplugin.debugOutput')}</Label>
                    <div className="max-h-64 overflow-auto rounded-lg bg-slate-900 p-3 font-mono text-xs">
                      {debugError ? (
                        <div className="text-rose-400">{debugError}</div>
                      ) : debugOutput ? (
                        <pre className="text-slate-300">{JSON.stringify(debugOutput, null, 2)}</pre>
                      ) : (
                        <div className="text-slate-600">{t('myplugin.noOutput')}</div>
                      )}
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm">{t('dag.config')}</CardTitle>
                </CardHeader>
                <CardContent className="space-y-2 text-xs text-muted-foreground">
                  <p>
                    Plugin config is read at runtime via <code className="text-primary">ctx.config</code>.
                  </p>
                  <p>Define config fields in your pipeline YAML:</p>
                  <pre className="mt-1 overflow-x-auto rounded bg-slate-900 p-2 text-slate-300">{`transforms:
  - type: plugin_${editorName}
    config:
      prefix: "prod"`}</pre>
                  <p className="pt-2">ABI manifest sent with install/compile:</p>
                  <pre className="mt-1 max-h-56 overflow-auto rounded bg-slate-900 p-2 text-slate-300">{editorManifest}</pre>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
