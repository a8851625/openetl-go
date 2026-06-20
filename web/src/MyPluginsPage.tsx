import React, { useState, useRef, useCallback, useEffect } from 'react';
import type { TFunc, Lang } from './types';
import Editor from '@monaco-editor/react';

type InstalledPlugin = {
  name: string;
  kind: string;
  wasm_path: string;
  version: string;
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
  const [tab, setTab] = useState<Tab>('installed');
  const [plugins, setPlugins] = useState<InstalledPlugin[]>([]);
  const [loading, setLoading] = useState(true);
  const [toast, setToast] = useState('');

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
  const fileRef = useRef<HTMLInputElement>(null);

  const showToast = useCallback((msg: string) => { setToast(msg); setTimeout(() => setToast(''), 4000); }, []);

  const refresh = useCallback(() => {
    setLoading(true);
    api<{ installed?: InstalledPlugin[] }>('/api/v2/plugins')
      .then((d) => setPlugins(d.installed || []))
      .catch(() => setPlugins([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  // ── Tab switching ──

  const switchToEditor = useCallback((source?: string, name?: string, kind?: PluginKind) => {
    setEditorSource(source ?? TEMPLATES.transform);
    setEditorName(name ?? 'my-transform');
    setEditorKind(kind ?? 'transform');
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
  }, [editorKind]);

  // Load the VIP order enricher example (a real-world plugin)
  const loadExample = useCallback((_kind: string) => {
    setEditorSource(VIP_EXAMPLE_SOURCE);
    setEditorName('vip-order-enricher');
    setEditorKind('transform');
  }, []);

  // ── Compile ──

  const handleCompile = useCallback(async () => {
    if (!editorName.trim() || !editorSource.trim()) {
      showToast(t('myplugin.enterNameAndSource') || 'Please enter a plugin name and source code');
      return;
    }
    setIsCompiling(true);
    try {
      const formData = new FormData();
      formData.append('source', editorSource);
      formData.append('name', editorName.trim());
      formData.append('kind', editorKind);
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
  }, [editorName, editorSource, editorKind, t, refresh, showToast]);

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

  // ── Upload WASM ──

  const handleUpload = useCallback(async () => {
    if (!selectedFile || !uploadName) { showToast(t('myplugin.selectFileAndName') || 'Please select a file and enter a name'); return; }
    try {
      const formData = new FormData();
      formData.append('wasm', selectedFile);
      formData.append('name', uploadName);
      formData.append('kind', uploadKind);
      formData.append('version', '1.0.0');
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
      if (fileRef.current) fileRef.current.value = '';
      refresh();
    } catch (e) {
      showToast(`Install failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, [selectedFile, uploadName, uploadKind, showToast, refresh]);

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
      switchToEditor(undefined, meta.name, meta.kind as PluginKind);
    } catch {
      switchToEditor(undefined, p.name, p.kind as PluginKind);
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

  return (
    <div className="space-y-6">
      {toast && (
        <div className="fixed right-4 top-20 z-50 rounded-lg bg-emerald-600 px-4 py-3 text-sm text-white shadow-lg">{toast}</div>
      )}

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-slate-200">
        <button className={`border-b-2 px-4 py-2.5 text-sm font-medium transition-colors ${tab === 'installed' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`} onClick={switchToInstalled}>
          {t('myplugin.tabInstalled')}
        </button>
        <button className={`border-b-2 px-4 py-2.5 text-sm font-medium transition-colors ${tab === 'editor' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`} onClick={() => switchToEditor()}>
          {t('myplugin.tabEditor')}
        </button>
      </div>

      {tab === 'installed' && (
        <>
          {/* Upload WASM card */}
          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('myplugin.upload')}</h2></div>
            <div className="card-body space-y-4">
              <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
                <div>
                  <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.pluginName')}</label>
                  <input className="input w-full" value={uploadName} onChange={(e) => setUploadName(e.target.value)} placeholder="my-transform" />
                </div>
                <div>
                  <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.pluginKind')}</label>
                  <select className="input w-full" value={uploadKind} onChange={(e) => setUploadKind(e.target.value as PluginKind)}>
                    {KIND_OPTIONS.map((k) => <option key={k} value={k}>{k}</option>)}
                  </select>
                </div>
                <div>
                  <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.selectFile')}</label>
                  <input ref={fileRef} type="file" accept=".wasm" className="block w-full text-sm text-slate-500 file:mr-3 file:rounded-md file:border-0 file:bg-indigo-50 file:px-3 file:py-1.5 file:text-xs file:font-medium file:text-indigo-700 hover:file:bg-indigo-100" onChange={(e) => setSelectedFile(e.target.files?.[0] || null)} />
                </div>
                <div className="flex items-end">
                  <button className="btn btn-primary w-full" onClick={handleUpload} disabled={!selectedFile || !uploadName}>{t('common.install')}</button>
                </div>
              </div>
            </div>
          </div>

          {/* Installed Plugins */}
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <h2 className="text-sm font-semibold">{t('myplugin.installed')}</h2>
              <div className="flex items-center gap-2">
                <button className="btn btn-secondary btn-sm" onClick={refresh}>{t('dlq.refresh')}</button>
                <button className="btn btn-primary btn-sm" onClick={() => switchToEditor()}>+ {t('myplugin.newPlugin')}</button>
              </div>
            </div>
            <div className="overflow-x-auto">
              {loading ? (
                <div className="p-8 text-center text-sm text-slate-400">Loading...</div>
              ) : plugins.length === 0 ? (
                <div className="p-8">
                  <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{t('myplugin.noPlugins')}</div>
                </div>
              ) : (
                <table className="tbl">
                  <thead>
                    <tr>
                      <th>{t('common.name')}</th>
                      <th>{t('common.kind')}</th>
                      <th>{t('common.version')}</th>
                      <th>{t('common.status')}</th>
                      <th>{t('myplugin.wasmPath')}</th>
                      <th>{t('common.actions')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {plugins.map((p) => (
                      <tr key={p.name}>
                        <td className="font-medium">{p.name}</td>
                        <td><span className={`badge ${p.kind === 'source' ? 'badge-cyan' : p.kind === 'sink' ? 'badge-emerald' : 'badge-violet'}`}>{p.kind}</span></td>
                        <td className="text-sm">v{p.version}</td>
                        <td><span className={`badge ${p.enabled ? 'badge-emerald' : 'badge-slate'}`}>{p.enabled ? t('common.enabled') : t('common.disabled')}</span></td>
                        <td className="max-w-xs truncate text-xs text-slate-400">{p.wasm_path}</td>
                        <td>
                          <div className="flex gap-1">
                            <button className="btn btn-secondary btn-sm" onClick={() => editPlugin(p)} title={t('pipe.edit')}>✏️</button>
                            <button className="btn btn-danger btn-sm" onClick={() => uninstall(p.name)}>{t('common.uninstall')}</button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          {/* SDK Info */}
          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('myplugin.sdk')}</h2></div>
            <div className="card-body">
              <p className="text-sm text-slate-600">{t('myplugin.sdkDesc')}</p>
              <div className="mt-3 rounded-lg bg-slate-900 p-4 text-sm text-slate-300">
                <div className="text-xs text-slate-500"># Install the SDK &amp; Extism JS PDK</div>
                <div className="font-mono">npm install @etl/sdk @extism/js-pdk</div>
                <div className="mt-3 text-xs text-slate-500"># Install the extism-js compiler</div>
                <div className="font-mono">npm install -g @extism/js-pdk</div>
                <div className="mt-3 text-xs text-slate-500"># Write your plugin (src/transform.ts)</div>
                <pre className="font-mono text-xs overflow-auto">{`import { createExtismTransformPlugin } from '@etl/sdk';

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
                <div className="mt-3 text-xs text-slate-500"># Compile to WASM &amp; install</div>
                <div className="font-mono">extism-js compile src/transform.ts -o dist/transform.wasm</div>
                <div className="font-mono mt-1"># Then upload dist/transform.wasm in the UI</div>
              </div>
            </div>
          </div>
        </>
      )}

      {tab === 'editor' && (
        <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
          {/* Left: Editor */}
          <div className="xl:col-span-2 space-y-4">
            {/* Toolbar */}
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold">{t('myplugin.editor')}</span>
                <span className="text-xs text-slate-400">{t('myplugin.editingFile')} {editorName}.ts</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-500">
                  {editorKind}/{editorVersion}
                </span>
                <button className="btn btn-ghost btn-sm" onClick={loadTemplate} title={t('ui.loadTemplate')}>{'📄 ' + t('common.template')}</button>
                <button className="btn btn-ghost btn-sm" onClick={() => loadExample('vip')} title={t('ui.loadExample')}>{'📦 ' + t('common.example')}</button>
                <button className="btn btn-secondary btn-sm" onClick={() => downloadSource(editorName, editorSource)}>📥 {t('myplugin.downloadSource')}</button>
                <button className={`btn btn-primary btn-sm ${isCompiling ? 'opacity-50' : ''}`} onClick={handleCompile} disabled={isCompiling}>
                  {isCompiling ? '⚙ ' + t('myplugin.compiling') : '⚙ ' + t('common.install')}
                </button>
              </div>
            </div>

            {/* Monaco Editor */}
            <div className="rounded-xl overflow-hidden border border-slate-200" style={{ minHeight: '450px' }}>
              <Editor
                height="450px"
                defaultLanguage="typescript"
                theme="etl-dark"
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

            {/* Plugin metadata */}
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.pluginName')}</label>
                <input className="input w-full text-sm" value={editorName} onChange={(e) => setEditorName(e.target.value)} placeholder="my-transform" />
              </div>
              <div>
                <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.pluginKind')}</label>
                <select className="input w-full text-sm" value={editorKind} onChange={(e) => handleKindChange(e.target.value as PluginKind)}>
                  {KIND_OPTIONS.map((k) => <option key={k} value={k}>{k}</option>)}
                </select>
              </div>
              <div>
                <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.pluginVersion')}</label>
                <input className="input w-full text-sm" value={editorVersion} onChange={(e) => setEditorVersion(e.target.value)} />
              </div>
            </div>
          </div>

          {/* Right: Debug panel + Config */}
          <div className="space-y-4 xl:col-span-1">
            {/* Debug */}
            <div className="card">
              <div className="card-header"><h2 className="text-sm font-semibold">{t('myplugin.debugTitle')}</h2></div>
              <div className="card-body space-y-3">
                <div>
                  <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.sampleRecord')}</label>
                  <textarea className="input w-full font-mono text-xs" rows={8}
                    value={sampleRecord}
                    onChange={(e) => setSampleRecord(e.target.value)}
                    placeholder={t('myplugin.pasteHere')} />
                </div>
                <button className="btn btn-primary btn-sm w-full" onClick={handleDebug} disabled={debugRunning || !editorName.trim()}>
                  {debugRunning ? '...' : '▶ ' + t('myplugin.runDebug')}
                </button>

                {/* Debug output */}
                <div>
                  <label className="mb-1.5 block text-xs font-medium text-slate-500">{t('myplugin.debugOutput')}</label>
                  <div className="rounded-lg bg-slate-900 p-3 font-mono text-xs max-h-64 overflow-auto">
                    {debugError ? (
                      <div className="text-rose-400">{debugError}</div>
                    ) : debugOutput ? (
                      <pre className="text-slate-300">{JSON.stringify(debugOutput, null, 2)}</pre>
                    ) : (
                      <div className="text-slate-600">{t('myplugin.noOutput')}</div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Config hints */}
            <div className="card">
              <div className="card-header"><h2 className="text-sm font-semibold">{t('dag.config')}</h2></div>
              <div className="card-body text-xs text-slate-500 space-y-2">
                <p>Plugin config is read at runtime via <code className="text-indigo-400">ctx.config</code>.</p>
                <p>Define config fields in your pipeline YAML:</p>
                <pre className="mt-1 rounded bg-slate-900 p-2 text-slate-300 overflow-x-auto">{`transforms:
  - type: plugin_${editorName}
    config:
      prefix: "prod"`}</pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
