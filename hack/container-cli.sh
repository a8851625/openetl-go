#!/bin/sh

# Shared container runtime selection for hack scripts.
# Set CONTAINER_CLI=docker or CONTAINER_CLI=podman to override detection.

has_container_cli() {
  if [ -n "${CONTAINER_CLI:-}" ]; then
    command -v "$CONTAINER_CLI" >/dev/null 2>&1
    return
  fi
  command -v podman >/dev/null 2>&1 || command -v docker >/dev/null 2>&1
}

detect_container_cli() {
  if [ -n "${CONTAINER_CLI:-}" ]; then
    if ! command -v "$CONTAINER_CLI" >/dev/null 2>&1; then
      echo "$CONTAINER_CLI is not available" >&2
      exit 2
    fi
    return
  fi
  if command -v podman >/dev/null 2>&1; then
    CONTAINER_CLI=podman
    return
  fi
  if command -v docker >/dev/null 2>&1; then
    CONTAINER_CLI=docker
    return
  fi
  echo "docker or podman is required" >&2
  exit 2
}

compose() {
  detect_container_cli
  if "$CONTAINER_CLI" compose version >/dev/null 2>&1; then
    "$CONTAINER_CLI" compose "$@"
    return
  fi
  cli_name="${CONTAINER_CLI##*/}"
  case "$cli_name" in
    docker)
      if command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
        return
      fi
      ;;
    podman)
      if command -v podman-compose >/dev/null 2>&1; then
        podman-compose "$@"
        return
      fi
      ;;
  esac
  echo "$CONTAINER_CLI compose is required" >&2
  exit 2
}

has_compose() {
  detect_container_cli
  if "$CONTAINER_CLI" compose version >/dev/null 2>&1; then
    return 0
  fi
  cli_name="${CONTAINER_CLI##*/}"
  case "$cli_name" in
    docker) command -v docker-compose >/dev/null 2>&1 ;;
    podman) command -v podman-compose >/dev/null 2>&1 ;;
    *) return 1 ;;
  esac
}
