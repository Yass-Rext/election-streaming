#!/usr/bin/env bash
# wait-for-it.sh — attend qu'un host:port soit ouvert (version allégée).
# Usage: wait-for-it.sh host:port [-t timeout] [-- command args]
set -euo pipefail

HOSTPORT="${1:-}"
shift || true

TIMEOUT=60
if [[ "${1:-}" == "-t" ]]; then
  TIMEOUT="${2:-60}"
  shift 2
fi

if [[ "${1:-}" == "--" ]]; then
  shift
fi

HOST="${HOSTPORT%%:*}"
PORT="${HOSTPORT##*:}"

echo "Attente ${HOST}:${PORT} (timeout ${TIMEOUT}s)..."
start=$(date +%s)
while true; do
  if (echo > "/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
    echo "${HOST}:${PORT} disponible"
    break
  fi
  now=$(date +%s)
  if (( now - start >= TIMEOUT )); then
    echo "Timeout en attendant ${HOST}:${PORT}" >&2
    exit 1
  fi
  sleep 1
done

if [[ "$#" -gt 0 ]]; then
  exec "$@"
fi
