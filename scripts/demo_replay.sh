#!/usr/bin/env bash
set -euo pipefail

URL="${CHRONICLE_URL:-http://127.0.0.1:8080}"

if [[ ! -f /tmp/chronicle_demo_base.txt ]]; then
  echo "missing /tmp/chronicle_demo_base.txt — run scripts/seed_demo.sh first" >&2
  exit 1
fi

if [[ -n "${STREAM:-}" ]]; then
  :
elif [[ -f /tmp/chronicle_demo_stream.txt ]]; then
  STREAM="$(cat /tmp/chronicle_demo_stream.txt)"
else
  STREAM="demo"
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
RESP="$(curl -fsS -X POST "${URL}/v1/replays" \
  -H 'content-type: application/json' \
  -d "{\"stream_id\":\"${STREAM}\",\"from\":\"${FROM}\",\"to\":\"${TO}\",\"speed\":\"10x\"}")"
echo "${RESP}" | tee /tmp/chronicle_demo_replay.json
REPLAY_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["replay_id"])' <<<"${RESP}")"

echo "Waiting for replay ${REPLAY_ID}..."
for _ in $(seq 1 60); do
  STATUS_JSON="$(curl -fsS "${URL}/v1/replays/${REPLAY_ID}")"
  STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${STATUS_JSON}")"
  EMITTED="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["events_emitted"])' <<<"${STATUS_JSON}")"
  echo "  status=${STATUS} events_emitted=${EMITTED}"
  case "${STATUS}" in
    completed|failed|cancelled)
      echo "${STATUS_JSON}"
      [[ "${STATUS}" == "completed" ]] || exit 1
      exit 0
      ;;
  esac
  sleep 0.5
done

echo "replay did not finish in time" >&2
exit 1
