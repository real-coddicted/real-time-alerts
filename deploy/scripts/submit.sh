#!/usr/bin/env bash
# Submits the alerting job to the Flink cluster.
#   submit.sh [--config <path inside the image>] [--from-savepoint <s3://...>] [extra pipeline options...]
# Defaults to the risk example. KAFKA_BOOTSTRAP / OFFSET_RESET / OUTPUT_INTERVAL_MS are passed to the
# client, where ${...} placeholders in the pipeline config are resolved.
set -euo pipefail
DEPLOY="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG=/opt/flink/usrlib/examples/risk/pipeline.json
SAVEPOINT=()
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    --from-savepoint) SAVEPOINT=(-s "$2"); shift 2 ;;
    *) EXTRA+=("$1"); shift ;;
  esac
done
docker compose -f "$DEPLOY/docker-compose.yml" exec -T \
  -e KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}" \
  -e OFFSET_RESET="${OFFSET_RESET:-latest}" \
  -e OUTPUT_INTERVAL_MS="${OUTPUT_INTERVAL_MS:-1000}" \
  jobmanager flink run -d "${SAVEPOINT[@]}" /opt/flink/usrlib/real-time-alerts.jar \
    --runner=FlinkRunner \
    --attachedMode=false \
    --config="$CONFIG" \
    --checkpointingInterval="${CHECKPOINT_INTERVAL_MS:-60000}" \
    --parallelism="${PARALLELISM:-2}" \
    "${EXTRA[@]}"
