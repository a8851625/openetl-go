#!/usr/bin/env bash
# Preserve the existing MinIO protocol fixture when its public images disappear.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
. "$ROOT/hack/container-cli.sh"
detect_container_cli
fixture_image=quay.io/minio/minio:RELEASE.2024-07-16T23-46-41Z
mirror_image=docker.io/minio/minio:RELEASE.2024-07-16T23-46-41Z
source_image=openetl-minio-fixture:2024-07-16
force_source=false
case "${1:-}" in
  '') ;;
  --force-source) force_source=true ;;
  *) echo 'usage: ensure-minio-fixture.sh [--force-source]' >&2; exit 2 ;;
esac
verify_fixture() {
  local server client
  server="$("$CONTAINER_CLI" run --rm --entrypoint /usr/bin/minio "$fixture_image" --version)" || return 1
  client="$("$CONTAINER_CLI" run --rm --entrypoint /usr/bin/mc "$fixture_image" --version)" || return 1
  printf '%s\n%s\n' "$server" "$client"
  [[ "$server" == *3535197f993dea840dcb96302ba4d883dc619097* &&
     "$client" == *11034f9de1e9f993c36fbad961f76f876a753328* ]]
}
if ! "$force_source"; then
  if "$CONTAINER_CLI" image inspect "$fixture_image" >/dev/null 2>&1 && verify_fixture; then exit 0; fi
  if "$CONTAINER_CLI" pull "$fixture_image"; then verify_fixture; exit 0; fi
  if "$CONTAINER_CLI" pull "$mirror_image"; then
    "$CONTAINER_CLI" tag "$mirror_image" "$fixture_image"
    verify_fixture
    exit 0
  fi
fi
"$CONTAINER_CLI" build -f "$ROOT/hack/Dockerfile.minio-fixture" -t "$source_image" "$ROOT/hack"
"$CONTAINER_CLI" tag "$source_image" "$fixture_image"
verify_fixture
