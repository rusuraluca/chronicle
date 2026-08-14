#!/usr/bin/env bash
set -euo pipefail

URL="${CHRONICLE_URL:-http://127.0.0.1:8080}"
# Fresh stream each run so prior ingest watermarks do not flag OOO.
STREAM="${STREAM:-demo-$(date -u +%Y%m%d%H%M%S)}"

echo "Waiting for Chronicle at ${URL}..."
for _ in $(seq 1 60); do
  if curl -fsS "${URL}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "${URL}/healthz" >/dev/null

python3 - <<PY
import json, urllib.request, datetime
url = "${URL}".rstrip("/")
stream = "${STREAM}"
base = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
for i in range(10):
    body = {
        "event_id": f"demo-{i}",
        "event_time": (base + datetime.timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
        "payload": {"n": i, "msg": f"hello-{i}"},
    }
    req = urllib.request.Request(
        f"{url}/v1/streams/{stream}/events",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print(resp.read().decode())
print("seeded 10 events on stream", stream)
print("BASE_TIME", base.isoformat().replace("+00:00", "Z"))
with open("/tmp/chronicle_demo_base.txt", "w") as f:
    f.write(base.isoformat().replace("+00:00", "Z"))
with open("/tmp/chronicle_demo_stream.txt", "w") as f:
    f.write(stream)
PY
