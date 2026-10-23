#!/usr/bin/env bash
# Produces an example scenario into Kafka, one event at a time, spaced so each one gets its own evaluation.
#   replay.sh <examples/<domain>> [delay-seconds]
set -euo pipefail
DEPLOY="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${1:?usage: replay.sh <examples/dir> [delay-seconds]}"
DELAY="${2:-1.5}"
python3 - "$DIR" <<'PY' | while IFS=$'\t' read -r topic record; do
import json, re, sys
d = sys.argv[1]
cfg = re.sub(r"\$\{[^}:]+(?::-([^}]*))?}", lambda m: m.group(1) or "0", open(f"{d}/pipeline.json").read())
topics = {s["name"]: s["topic"] for s in json.loads(cfg)["sources"]}
for line in open(f"{d}/scenario.jsonl"):
    line = line.strip()
    if not line or line.startswith("//"):
        continue
    env = json.loads(line)
    print(topics[env["source"]] + "\t" + json.dumps(env["record"], separators=(",", ":")))
PY
  echo "$record" | docker compose -f "$DEPLOY/docker-compose.yml" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$topic" >/dev/null
  echo "-> $topic"
  sleep "$DELAY"
done
