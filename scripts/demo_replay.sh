#!/usr/bin/env bash
set -euo pipefail

URL="${CHRONICLE_URL:-http://127.0.0.1:8080}"
STREAM="${STREAM:-demo}"

if [[ ! -f /tmp/chronicle_demo_base.txt ]]; then
  echo "missing /tmp/chronicle_demo_base.txt — run scripts/seed_demo.sh first" >&2
  exit 1
fi

FROM="$(cat /tmp/chronicle_demo_base.txt)"
# 10 seeded events at 1s spacing → cover [base, base+15s]
TO="$(python3 - <<PY
from datetime import datetime, timedelta, timezone
base = datetime.fromisoformat("${FROM}".replace("Z", "+00:00"))
print((base + timedelta(seconds=15)).isoformat().replace("+00:00", "Z"))
PY
)"

echo "Starting 10x replay on ${STREAM} from ${FROM} to ${TO}"
curl -fsS -X POST "${URL}/v1/replays" \
  -H 'content-type: application/json' \
  -d "{\"stream_id\":\"${STREAM}\",\"from\":\"${FROM}\",\"to\":\"${TO}\",\"speed\":\"10x\"}" \
  | tee /tmp/chronicle_demo_replay.json

echo
echo "Poll with: curl -s ${URL}/v1/replays/\$(jq -r .replay_id /tmp/chronicle_demo_replay.json)"
