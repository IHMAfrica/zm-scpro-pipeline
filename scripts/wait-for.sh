#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-}"
PORT="${2:-}"
TIMEOUT="${3:-60}"

if [[ -z "$HOST" || -z "$PORT" ]]; then
  echo "Usage: wait-for <host> <port> [timeout_seconds]"
  exit 64
fi

echo "Waiting for $HOST:$PORT (timeout ${TIMEOUT}s)..."
end=$((SECONDS+TIMEOUT))
while ! nc -z "$HOST" "$PORT" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for $HOST:$PORT"
    exit 1
  fi
  sleep 1
done
echo "$HOST:$PORT is up."
