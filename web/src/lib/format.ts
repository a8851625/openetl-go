export function ratio(a: number, b: number) {
  return b <= 0 ? 0 : a / b;
}

export function fmtTime(v?: string) {
  if (!v || v.startsWith('0001-')) return 'n/a';
  return new Date(v).toLocaleString();
}

export function formatDuration(seconds: number): string {
  if (seconds < 0 || !isFinite(seconds)) return 'N/A';
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export function parseStartedAt(startedAt: string | undefined): number | null {
  if (!startedAt) return null;
  const t = new Date(startedAt).getTime();
  return isNaN(t) ? null : t;
}

export function prettyJSON(value: unknown) {
  return JSON.stringify(value, null, 2);
}

export function parseJSONText(text: string, fallback: unknown) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

export function parseJSONObject(text: string): Record<string, unknown> {
  const parsed = parseJSONText(text, {});
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}
