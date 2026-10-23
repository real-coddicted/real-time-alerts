#!/usr/bin/env bash
# One-time setup of the local single-node Garage: cluster layout, the checkpoint bucket and Flink's access key.
# Idempotent: safe to re-run.
set -euo pipefail
DEPLOY="$(cd "$(dirname "$0")/.." && pwd)"
set -a; source "$DEPLOY/.env"; set +a
garage() { docker compose -f "$DEPLOY/docker-compose.yml" exec -T -e RUST_LOG=warn garage /garage "$@"; }

for _ in $(seq 1 30); do garage status >/dev/null 2>&1 && break; sleep 1; done
NODE_ID="$(garage node id -q | cut -d@ -f1)"

if garage layout show | grep -q "No nodes currently have a role"; then
  garage layout assign -z dc1 -c 10G "$NODE_ID"
  garage layout apply --version 1
fi
garage key info "$GARAGE_ACCESS_KEY" >/dev/null 2>&1 || garage key import --yes -n flink "$GARAGE_ACCESS_KEY" "$GARAGE_SECRET_KEY" >/dev/null  # output includes the secret
garage bucket info "$GARAGE_BUCKET" >/dev/null 2>&1 || garage bucket create "$GARAGE_BUCKET"
garage bucket allow --read --write --owner "$GARAGE_BUCKET" --key "$GARAGE_ACCESS_KEY" >/dev/null
echo "garage ready: bucket '$GARAGE_BUCKET', key $GARAGE_ACCESS_KEY"
