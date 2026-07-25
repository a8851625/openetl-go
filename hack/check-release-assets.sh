#!/usr/bin/env bash
# P5 release gate: production assets must not ship empty tokens, change-me
# placeholders, or floating :latest image defaults.
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FAIL=0
report() {
  echo "FAIL: $*" >&2
  FAIL=$((FAIL + 1))
}

echo "==> production compose / deploy assets must pin image and require secrets"

# Root production compose: OPENETL_IMAGE required, no :latest default, no change-me.
if grep -E 'OPENETL_IMAGE:-.*:latest|image:.*:latest' docker-compose.yml >/dev/null 2>&1; then
  report "docker-compose.yml references floating :latest image"
fi
if grep -E 'change-me' docker-compose.yml >/dev/null 2>&1; then
  report "docker-compose.yml contains change-me placeholder"
fi
if ! grep -E 'OPENETL_IMAGE:\?OPENETL_IMAGE' docker-compose.yml >/dev/null 2>&1; then
  report "docker-compose.yml must require OPENETL_IMAGE (no silent default)"
fi
if ! grep -E 'ETL_API_TOKEN:\?ETL_API_TOKEN' docker-compose.yml >/dev/null 2>&1; then
  report "docker-compose.yml must require ETL_API_TOKEN"
fi

# deploy/production package
if grep -E 'OPENETL_IMAGE:-.*:latest|image:.*[^v0-9].*:latest' deploy/production/docker-compose.yml >/dev/null 2>&1; then
  report "deploy/production/docker-compose.yml uses :latest default"
fi
if grep -E 'ETL_API_TOKEN:-\s*$|ETL_API_TOKEN:-\s*"?"?' deploy/production/docker-compose.yml >/dev/null 2>&1; then
  report "deploy/production/docker-compose.yml allows empty ETL_API_TOKEN default"
fi
if ! grep -E 'ETL_API_TOKEN:\?' deploy/production/docker-compose.yml >/dev/null 2>&1; then
  report "deploy/production/docker-compose.yml must require ETL_API_TOKEN"
fi
if grep -E 'change-me' deploy/production/docker-compose.yml deploy/production/.env.example >/dev/null 2>&1; then
  report "deploy/production compose/env.example contains change-me"
fi
# config.yaml may document examples but must not be the live default path without env override notes;
# still ban bare change-me in committed production config used by compose mounts.
if grep -E 'change-me' deploy/production/config.yaml >/dev/null 2>&1; then
  report "deploy/production/config.yaml contains change-me (use empty + env override)"
fi

# manifest production example must not advertise latest / change-me as defaults
if grep -E ':latest|change-me' manifest/examples/config.production.yaml >/dev/null 2>&1; then
  report "manifest/examples/config.production.yaml contains latest/change-me"
fi

# goreleaser may publish :latest as an additional tag for GHCR convenience, but
# production compose must never default to it. Document residual explicitly.
if grep -E 'image_templates:' -A5 .goreleaser.yml | grep -q ':latest'; then
  echo "NOTE: .goreleaser.yml still publishes :latest as a secondary tag (allowed); production consumers must pin version/digest."
fi

# distributed compose is beta — must not claim production; placeholders are residual until PR-D1.
if ! grep -Eiq 'beta|PR-D1|candidate' docker-compose.distributed.yml; then
  report "docker-compose.distributed.yml must label itself beta / PR-D1 residual"
fi

echo "==> docs release checklist present"
if [[ ! -f docs/release-checklist.md ]]; then
  report "missing docs/release-checklist.md"
fi
if [[ ! -f docs/ops-runbook.md ]]; then
  report "missing docs/ops-runbook.md"
fi
if [[ ! -f docs/resource-baseline.md ]]; then
  report "missing docs/resource-baseline.md"
fi

if [[ "$FAIL" -gt 0 ]]; then
  echo "release asset check failed: $FAIL issue(s)" >&2
  exit 1
fi
echo "release asset check passed"
