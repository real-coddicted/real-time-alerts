#!/usr/bin/env bash
# Zero-loss redeploy: stop the running job with a savepoint (to Garage), then resubmit from it so active
# alerts, throttle state and Kafka offsets carry over. Rebuild the image first if the jar changed:
#   ./gradlew shadowJar && docker compose -f deploy/docker-compose.yml up -d --build
#   ./deploy/scripts/upgrade.sh [submit.sh options...]
set -euo pipefail
DEPLOY="$(cd "$(dirname "$0")/.." && pwd)"
flink() { docker compose -f "$DEPLOY/docker-compose.yml" exec -T jobmanager flink "$@"; }

JOB_ID="$(flink list -r 2>/dev/null | awk '/RUNNING/ {print $4; exit}')"
if [[ -z "$JOB_ID" ]]; then
  echo "no running job; submitting fresh"
  exec "$DEPLOY/scripts/submit.sh" "$@"
fi
echo "stopping $JOB_ID with a savepoint..."
SAVEPOINT="$(flink stop "$JOB_ID" | sed -n 's/^Savepoint completed. Path: //p')"
[[ -n "$SAVEPOINT" ]] || { echo "savepoint failed; job left as is" >&2; exit 1; }
echo "savepoint: $SAVEPOINT"
exec "$DEPLOY/scripts/submit.sh" --from-savepoint "$SAVEPOINT" "$@"
