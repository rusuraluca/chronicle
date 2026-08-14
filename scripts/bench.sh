#!/usr/bin/env bash
set -euo pipefail

URL="${CHRONICLE_URL:-http://127.0.0.1:8080}"
COUNT="${COUNT:-1000}"
STREAM="${STREAM:-bench}"

echo "Waiting for ${URL}/healthz ..."
for _ in $(seq 1 60); do
  if curl -fsS "${URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "Running bench count=${COUNT} stream=${STREAM}"
curl -fsS -X POST "${URL}/v1/bench" \
  -H 'content-type: application/json' \
  -d "{\"count\":${COUNT},\"stream_id\":\"${STREAM}\"}"
echo
