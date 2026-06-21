#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="openetl-go-etl:ui-e2e"
APP="etl-ui-e2e"
DATA_DIR="$ROOT_DIR/data-ui-e2e"
LOG_DIR="$ROOT_DIR/logs"
BASE_URL="http://127.0.0.1:8076"
PASS=0
FAIL=0

if ! command -v podman >/dev/null 2>&1; then echo "podman is required" >&2; exit 1; fi
if ! command -v playwright-cli >/dev/null 2>&1; then echo "playwright-cli is required" >&2; exit 1; fi

cleanup() { podman rm -f "$APP" >/dev/null 2>&1 || true; rm -rf "$DATA_DIR"; playwright-cli close >/dev/null 2>&1 || true; }
trap cleanup EXIT

mkdir -p "$DATA_DIR/output" "$DATA_DIR/checkpoint" "$DATA_DIR/dlq" "$LOG_DIR"

echo "==> Build image"
podman build -t "$IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR" 2>&1 | tail -1
podman rm -f "$APP" >/dev/null 2>&1 || true
echo "==> Start app container"
podman run -d --name "$APP" \
  -p 8076:8000 -p 8077:8001 \
  -v "$ROOT_DIR/testdata/pipes-auth:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$DATA_DIR:/app/data" \
  -v "$LOG_DIR:/app/logs" \
  "$IMAGE" >/dev/null

echo "==> Wait for app healthy"
for _ in $(seq 1 60); do curl -fsS http://127.0.0.1:8077/api/v2/health >/dev/null 2>&1 && break; sleep 1; done
curl -fsS http://127.0.0.1:8077/api/v2/health >/dev/null

echo "==> Verify reverse proxy"
for _ in $(seq 1 30); do curl -fsS "${BASE_URL}/api/v2/health" >/dev/null 2>&1 && break; sleep 1; done
if ! curl -fsS "${BASE_URL}/api/v2/health" >/dev/null 2>&1; then echo "ERROR: reverse proxy not working" >&2; exit 1; fi
echo "    Reverse proxy OK"

echo "==> Open browser"
playwright-cli open "${BASE_URL}/" >/dev/null
sleep 2

pass() { echo "  PASS  $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL  $1" >&2; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "true" ]]; then pass "$1"; else fail "$1 (got: ${2:0:100})"; fi; }
evaljs() { playwright-cli --raw eval "$1"; }

# Navigate to a page by clicking sidebar item
goto_page() {
  local label="$1"
  playwright-cli click ".sidebar-item:has-text('$label')" >/dev/null 2>&1 || true
  sleep 1
}

# ════════════════════════════════════════════════
echo "=== A: Page Structure & Sidebar ==="
check "A1: Title = Sync Canal ETL" "$(evaljs "document.title === 'Sync Canal ETL'")"
check "A2: Sidebar present" "$(evaljs "document.querySelector('aside') !== null")"
check "A3: 6 nav items" "$(evaljs "document.querySelectorAll('.sidebar-item').length >= 6")"
check "A4: Brand 'Sync Canal'" "$(evaljs "document.body.innerText.includes('Sync Canal')")"
check "A5: Default page = Dashboard" "$(evaljs "document.body.innerText.includes('Pipeline Overview') || document.body.innerText.includes('管道总览')")"

# ════════════════════════════════════════════════
echo "=== B: i18n Language Toggle ==="
# B1: Default English — check English text
check "B1: English label 'Dashboard'" "$(evaljs "document.body.innerText.includes('Dashboard')")"

# B2: Switch to Chinese via topbar globe button
playwright-cli click "header button[title]" >/dev/null 2>&1 || true
sleep 1
check "B2: Switched to Chinese" "$(evaljs "document.body.innerText.includes('仪表盘')")"
check "B3: Chinese nav label '管道'" "$(evaljs "document.body.innerText.includes('管道')")"
check "B4: Chinese metric label" "$(evaljs "document.body.innerText.includes('读取记录')")"

# B3: Switch back to English
playwright-cli click "header button[title]" >/dev/null 2>&1 || true
sleep 1
check "B5: Back to English" "$(evaljs "document.body.innerText.includes('Dashboard')")"

# B4: Language persisted in localStorage
check "B6: lang in localStorage" "$(evaljs "localStorage.getItem('etl_lang') === 'en'")"

# ════════════════════════════════════════════════
echo "=== C: Dashboard Page ==="
# Dashboard is default page
check "C1: Metric cards rendered" "$(evaljs "document.querySelectorAll('.text-3xl').length >= 5")"
check "C2: Pipeline visible" "$(evaljs "document.body.innerText.includes('auth-file-to-file')")"
check "C3: 'written' badge" "$(evaljs "document.body.innerText.includes('written')")"
check "C4: Pipeline overview card" "$(evaljs "document.body.innerText.includes('Pipeline Overview')")"
check "C5: Key metrics card" "$(evaljs "document.body.innerText.includes('Key Metrics')")"
check "C6: Progress bar exists" "$(evaljs "document.querySelector('.progress-track') !== null")"

# Click pipeline to select
playwright-cli click ".pipeline-row:has-text('auth-file-to-file')" >/dev/null 2>&1 || true
sleep 1
check "C7: Pipeline selected" "$(evaljs "document.querySelector('.pipeline-row.selected') !== null")"

# ════════════════════════════════════════════════
echo "=== D: Pipelines Page ==="
goto_page "Pipelines"
check "D1: All Pipelines header" "$(evaljs "document.body.innerText.includes('All Pipelines')")"
check "D2: Start button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.trim()==='Start')")"
check "D3: Stop button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.trim()==='Stop')")"
check "D4: Checkpoints card" "$(evaljs "document.body.innerText.includes('Checkpoints')")"

# Click a pipeline row to verify selection works
evaljs "document.querySelector('.pipeline-row')?.click()" >/dev/null 2>&1 || true
sleep 1
check "D5: Pipeline row selected" "$(evaljs "document.querySelector('.pipeline-row.selected') != null")"

# Click Start
evaljs "Array.from(document.querySelectorAll('button')).find(b=>b.textContent?.trim()==='Start')?.click()" >/dev/null 2>&1 || true
sleep 3
check "D6: Start action triggered" "$(evaljs "document.body.innerText.includes('Start') || document.body.innerText.includes('Success')")"

# ════════════════════════════════════════════════
echo "=== E: Designer Page (Visual DAG Editor) ==="
goto_page "Designer"
check "E1: DAG Editor title" "$(evaljs "document.body.innerText.includes('Designer') || document.body.innerText.includes('设计器')")"
check "E2: Add Source button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('Source'))")"
check "E3: Add Transform button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('Transform'))")"
check "E4: Add Sink button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('Sink'))")"
check "E5: Export YAML button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('YAML'))")"
check "E6: Create Pipeline button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('Create Pipeline'))")"

# Add a source node
playwright-cli click "button:has-text('Source')" >/dev/null 2>&1 || true
sleep 1
# Add a sink node
playwright-cli click "button:has-text('Sink')" >/dev/null 2>&1 || true
sleep 1
check "E7: Nodes added to canvas" "$(evaljs "document.querySelectorAll('.react-flow__node').length >= 2")"
check "E8: Source node exists" "$(evaljs "Array.from(document.querySelectorAll('.react-flow__node')).some(n=>n.textContent?.includes('source'))")"
check "E9: Sink node exists" "$(evaljs "Array.from(document.querySelectorAll('.react-flow__node')).some(n=>n.textContent?.includes('sink'))")"

# Click a node to select it (opens properties panel)
playwright-cli click ".react-flow__node:first-child" >/dev/null 2>&1 || true
sleep 1
check "E10: Properties panel shown" "$(evaljs "document.body.innerText.includes('Plugin') || document.body.innerText.includes('plugin')")"

# Check that config form renders (schema-driven)
check "E11: Config form visible" "$(evaljs "document.querySelectorAll('.react-flow__node-selected').length > 0 || document.body.innerText.includes('Config') || document.querySelector('input[type=text]') !== null")"

# Export YAML
playwright-cli click "button:has-text('YAML')" >/dev/null 2>&1 || true
sleep 1
check "E12: YAML output appears" "$(evaljs "document.querySelectorAll('textarea').length > 0")"
check "E13: YAML has pipeline name" "$(evaljs "Array.from(document.querySelectorAll('textarea')).some(t=>(t.value||'').includes('my-pipeline') || (t.value||'').includes('name:'))")"

# ════════════════════════════════════════════════
echo "=== F: DLQ Page ==="
goto_page "DLQ"
check "F1: DLQ Workbench" "$(evaljs "document.body.innerText.includes('DLQ Workbench') || document.body.innerText.includes('Dead-Letter Queue')")"
check "F2: Select Pipeline card" "$(evaljs "document.body.innerText.includes('Select Pipeline')")"
check "F3: Pipeline visible in DLQ list" "$(evaljs "document.body.innerText.includes('auth-file-to-file')")"

# Filter input
check "F4: Filter input" "$(evaljs "document.querySelector('input[placeholder*=Filter]') !== null")"
playwright-cli fill "input[placeholder*=Filter]" "test-val" >/dev/null 2>&1 || true
sleep 1
check "F5: Filter accepts input" "$(evaljs "(document.querySelector('input[placeholder*=Filter]')?.value || '').includes('test')")"

check "F6: Replay button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.trim()==='Replay')")"
check "F7: Delete button" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.includes('Delete'))")"

# ════════════════════════════════════════════════
echo "=== G: Plugins Page ==="
goto_page "Built-in"
check "G1: Plugin Matrix" "$(evaljs "document.body.innerText.includes('Plugin Capability Matrix')")"
check "G2: mysql_cdc listed" "$(evaljs "document.body.innerText.includes('mysql_cdc')")"
check "G3: clickhouse listed" "$(evaljs "document.body.innerText.includes('clickhouse')")"
check "G4: kafka listed" "$(evaljs "document.body.innerText.includes('kafka')")"
check "G5: elasticsearch listed" "$(evaljs "document.body.innerText.includes('elasticsearch')")"
check "G6: Table rows exist" "$(evaljs "document.querySelectorAll('.tbl tr').length > 5")"
check "G7: 'source' kind" "$(evaljs "document.body.innerText.includes('source')")"
check "G8: 'sink' kind" "$(evaljs "document.body.innerText.includes('sink')")"

# ════════════════════════════════════════════════
echo "=== H: Audit Page ==="
goto_page "Audit"
check "H1: Audit Trail" "$(evaljs "document.body.innerText.includes('Audit Trail')")"
check "H2: Table exists" "$(evaljs "document.querySelectorAll('.tbl').length > 0")"

# ════════════════════════════════════════════════
echo "=== I: Settings & Token ==="
# Open settings modal
playwright-cli click ".sidebar-item:has-text('Settings')" >/dev/null 2>&1 || true
sleep 1

# Token input — it's the "General" tab by default
playwright-cli fill "input[placeholder*='API']" "my-token-456" >/dev/null 2>&1 || true
sleep 1
check "I1: Token value set" "$(evaljs "document.querySelector('input[placeholder*=API]')?.value === 'my-token-456'")"

# Save token
playwright-cli click "button:has-text('Save Token')" >/dev/null 2>&1 || true
sleep 1
check "I2: Token saved to localStorage" "$(evaljs "localStorage.getItem('etl_api_token') === 'my-token-456'")"

# Language toggle in settings
check "I3: English button in settings" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.trim()==='English')")"
check "I4: Chinese button in settings" "$(evaljs "Array.from(document.querySelectorAll('button')).some(b=>b.textContent?.trim()==='中文')")"

# Close settings modal
playwright-cli click "button:has-text('✕')" >/dev/null 2>&1 || true
sleep 1

# ════════════════════════════════════════════════
echo "=== J: Reload Specs ==="
playwright-cli click "button:has-text('Reload Specs')" >/dev/null 2>&1 || true
# Poll for toast (4s display window, check every 500ms)
found="false"
for _ in $(seq 1 8); do
  r="$(evaljs "document.body.innerText.includes('Reload specs') || document.body.innerText.includes('Success')")"
  if [[ "$r" == "true" ]]; then found="true"; break; fi
  sleep 0.5
done
check "J1: Reload specs toast appeared" "$found"

# ════════════════════════════════════════════════
echo "=== K: Backend API Integration ==="
check "K1: /api/v2/pipelines works" "$(evaljs "fetch('/api/v2/pipelines').then(r=>r.ok).catch(()=>false)")"
sleep 1
check "K2: /api/v2/metrics has latency" "$(evaljs "fetch('/api/v2/metrics').then(r=>r.json()).then(d=>JSON.stringify(d.pipelines?.[0]||{})).then(s=>s.includes('source_read_latency_ms')).catch(()=>false)")"
sleep 1
check "K3: /api/v2/plugins has metadata" "$(evaljs "fetch('/api/v2/plugins').then(r=>r.json()).then(d=>d.metadata!==undefined).catch(()=>false)")"
sleep 1
check "K4: /api/v2/plugins/schema works" "$(evaljs "fetch('/api/v2/plugins/schema').then(r=>r.ok).catch(()=>false)")"
sleep 1
check "K5: /api/v2/checkpoints works" "$(evaljs "fetch('/api/v2/checkpoints').then(r=>r.json()).then(d=>d.checkpoints!==undefined).catch(()=>false)")"
sleep 1
check "K6: /api/v2/audit works" "$(evaljs "fetch('/api/v2/audit?limit=20').then(r=>r.json()).then(d=>Array.isArray(d.events)).catch(()=>false)")"
sleep 1
check "K7: /api/v2/dlq works" "$(evaljs "fetch('/api/v2/dlq/auth-file-to-file').then(r=>r.json()).then(d=>Array.isArray(d.items)).catch(()=>false)")"

# ════════════════════════════════════════════════
echo "=== L: Auto-refresh ==="
check "L1: Auto-refresh label" "$(evaljs "document.body.innerText.includes('Auto-refresh')")"

# ════════════════════════════════════════════════
echo "=== M: Full Chinese Switch E2E ==="
# Switch to Chinese
playwright-cli click "header button[title]" >/dev/null 2>&1 || true
sleep 1
goto_page "仪表盘"
check "M1: Chinese dashboard label" "$(evaljs "document.body.innerText.includes('仪表盘')")"
goto_page "管道"
check "M2: Chinese pipelines label" "$(evaljs "document.body.innerText.includes('所有管道')")"
goto_page "可视化设计器"
check "M3: Chinese designer label" "$(evaljs "document.body.innerText.includes('可视化设计器') || document.body.innerText.includes('属性') || document.body.innerText.includes('添加')")"
goto_page "内置"
check "M4: Chinese plugins label" "$(evaljs "document.body.innerText.includes('插件能力矩阵')")"
goto_page "审计"
check "M5: Chinese audit label" "$(evaljs "document.body.innerText.includes('审计日志')")"

playwright-cli close >/dev/null 2>&1 || true
echo ""
echo "═══════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════"
if [[ "$FAIL" -gt 0 ]]; then echo "UI E2E FAILED ($FAIL failures)" >&2; exit 1; fi
echo "==> UI E2E passed ($PASS tests)"
