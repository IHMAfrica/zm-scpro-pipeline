#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash wait-for.sh "PLAINTEXT://kafka:9092" 120
#   bash wait-for.sh "kafka:9092" 120
endpoint="${1:-kafka:9092}"
timeout="${2:-120}"

# Strip scheme if present (PLAINTEXT:// or SASL_PLAINTEXT:// etc.)
endpoint="${endpoint#*://}"
host="${endpoint%:*}"
port="${endpoint##*:}"

echo "Waiting for $host:$port (timeout ${timeout}s)..."

start=$(date +%s)
while true; do
  # Use bash's /dev/tcp to avoid needing netcat
  if (exec 3<>/dev/tcp/"$host"/"$port") 2>/dev/null; then
    exec 3>&- 3<&-
    echo "OK: $host:$port is reachable."
    break
  fi

  now=$(date +%s)
  if (( now - start >= timeout )); then
    echo "ERROR: timeout after ${timeout}s waiting for $host:$port" >&2
    exit 1
  fi
  sleep 2
done
