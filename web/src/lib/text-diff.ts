/**
 * Lightweight line-oriented diff (LCS) for pipeline version comparison.
 * Pure TypeScript — no extra dependency.
 */

export type DiffOp = 'equal' | 'remove' | 'add';

export type DiffLine = {
  op: DiffOp;
  /** Content without trailing newline. */
  text: string;
  /** 1-based line number in historical (left) side; null for pure adds. */
  leftNo: number | null;
  /** 1-based line number in current (right) side; null for pure removes. */
  rightNo: number | null;
};

/** Split text into lines, preserving empty trailing line only when content ends with \n. */
export function splitLines(text: string): string[] {
  if (text === '') return [];
  // Keep empty last line only if the source ends with a newline (common for YAML dumps).
  const endsWithNl = text.endsWith('\n');
  const parts = text.split('\n');
  if (endsWithNl && parts[parts.length - 1] === '') {
    parts.pop();
  }
  return parts;
}

/**
 * Compute a line-level LCS diff.
 * Equal lines are kept so the viewer can show surrounding context;
 * UI may collapse long equal runs if needed.
 */
export function diffLines(historical: string, current: string): DiffLine[] {
  const a = splitLines(historical);
  const b = splitLines(current);
  const n = a.length;
  const m = b.length;

  // dp[i][j] = LCS length of a[i:] and b[j:]
  // Use 2D for clarity; pipeline YAML is small (typically << 2k lines).
  const dp: number[][] = Array.from({ length: n + 1 }, () => Array(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      if (a[i] === b[j]) {
        dp[i][j] = dp[i + 1][j + 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
  }

  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  let leftNo = 1;
  let rightNo = 1;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      out.push({ op: 'equal', text: a[i], leftNo: leftNo++, rightNo: rightNo++ });
      i++;
      j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      out.push({ op: 'remove', text: a[i], leftNo: leftNo++, rightNo: null });
      i++;
    } else {
      out.push({ op: 'add', text: b[j], leftNo: null, rightNo: rightNo++ });
      j++;
    }
  }
  while (i < n) {
    out.push({ op: 'remove', text: a[i++], leftNo: leftNo++, rightNo: null });
  }
  while (j < m) {
    out.push({ op: 'add', text: b[j++], leftNo: null, rightNo: rightNo++ });
  }
  return out;
}

export type SideBySideRow = {
  left: { no: number | null; text: string; op: DiffOp } | null;
  right: { no: number | null; text: string; op: DiffOp } | null;
};

/**
 * Pair remove/add into side-by-side rows so both panes stay aligned.
 * Consecutive remove+add are paired as a modification; pure remove/add get empty opposite.
 */
export function toSideBySide(lines: DiffLine[]): SideBySideRow[] {
  const rows: SideBySideRow[] = [];
  let k = 0;
  while (k < lines.length) {
    const line = lines[k];
    if (line.op === 'equal') {
      rows.push({
        left: { no: line.leftNo, text: line.text, op: 'equal' },
        right: { no: line.rightNo, text: line.text, op: 'equal' },
      });
      k++;
      continue;
    }

    // Collect a run of removes, then a run of adds (or vice versa) and pair them.
    const removes: DiffLine[] = [];
    const adds: DiffLine[] = [];
    while (k < lines.length && lines[k].op === 'remove') {
      removes.push(lines[k++]);
    }
    while (k < lines.length && lines[k].op === 'add') {
      adds.push(lines[k++]);
    }
    // If we hit equal after removes only, loop continues next iteration.
    // If an add-run appears before removes (shouldn't with LCS backtrack order,
    // but handle for robustness), collect remaining adds.
    while (k < lines.length && lines[k].op === 'add' && removes.length === 0) {
      adds.push(lines[k++]);
    }

    const max = Math.max(removes.length, adds.length);
    for (let t = 0; t < max; t++) {
      const r = removes[t];
      const a = adds[t];
      rows.push({
        left: r ? { no: r.leftNo, text: r.text, op: 'remove' } : null,
        right: a ? { no: a.rightNo, text: a.text, op: 'add' } : null,
      });
    }
  }
  return rows;
}
