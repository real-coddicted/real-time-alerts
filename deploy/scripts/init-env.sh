#!/usr/bin/env bash
# Creates deploy/.env with freshly generated secrets for the local Garage. Never committed (.gitignore: .env).
# For an existing Garage cluster, set S3_ENDPOINT, S3_REGION, GARAGE_BUCKET and the access key pair instead.
set -euo pipefail
ENV_FILE="$(cd "$(dirname "$0")/.." && pwd)/.env"
if [[ -f "$ENV_FILE" ]]; then
  echo "$ENV_FILE already exists; leaving it unchanged"
  exit 0
fi
umask 077
cat > "$ENV_FILE" <<ENV
GARAGE_RPC_SECRET=$(openssl rand -hex 32)
GARAGE_ADMIN_TOKEN=$(openssl rand -hex 32)
GARAGE_ACCESS_KEY=GK$(openssl rand -hex 12)
GARAGE_SECRET_KEY=$(openssl rand -hex 32)
GARAGE_BUCKET=flink
S3_ENDPOINT=http://garage:3900
S3_REGION=garage
PARALLELISM=2
ENV
echo "wrote $ENV_FILE"
