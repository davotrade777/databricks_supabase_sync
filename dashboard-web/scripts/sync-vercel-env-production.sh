#!/usr/bin/env bash
# Sync Production env from transportistas_sync/.env + AWS CLI profile (run from repo root or dashboard-web).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/dashboard-web"
SCOPE="${VERCEL_SCOPE:-adelca-lineas-transportistas}"

set -a
# shellcheck disable=SC1091
source "$ROOT/transportistas_sync/.env"
set +a

AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id 2>/dev/null || true)"
AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key 2>/dev/null || true)"
export AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || echo us-east-1)}"
export LAMBDA_NAME="${LAMBDA_NAME:-patek-philippe}"
export ETL_LOGS_BUCKET="${ETL_LOGS_BUCKET:-patek-philippe-etl-logs-052124708820}"
export ETL_SUCCESS_PREFIX="${ETL_SUCCESS_PREFIX:-etl-success}"

vc() {
  local name="$1"
  local value="$2"
  echo "→ $name"
  npx vercel@latest env add "$name" production --value "$value" --yes --force --scope "$SCOPE" --cwd "$(pwd -P)"
}

vc AWS_REGION "$AWS_REGION"
vc LAMBDA_NAME "$LAMBDA_NAME"
vc SUPABASE_URL "$SUPABASE_URL"
vc SUPABASE_SERVICE_ROLE_KEY "$SUPABASE_SERVICE_ROLE_KEY"
vc DATABRICKS_PRD_HOST "$DATABRICKS_PRD_HOST"
vc DATABRICKS_PRD_HTTP_PATH "$DATABRICKS_PRD_HTTP_PATH"
vc DATABRICKS_PRD_CLIENT_ID "$DATABRICKS_PRD_CLIENT_ID"
vc DATABRICKS_PRD_CLIENT_SECRET "$DATABRICKS_PRD_CLIENT_SECRET"
vc DATABRICKS_QAS_HOST "${DATABRICKS_QAS_HOST:-}"
vc DATABRICKS_QAS_HTTP_PATH "${DATABRICKS_QAS_HTTP_PATH:-}"
vc DATABRICKS_QAS_TOKEN "${DATABRICKS_QAS_TOKEN:-}"
vc SUPABASE_SECONDARY_URL "${SUPABASE_SECONDARY_URL:-}"
vc SUPABASE_SECONDARY_SERVICE_ROLE_KEY "${SUPABASE_SECONDARY_SERVICE_ROLE_KEY:-}"
vc AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID:-}"
vc AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY:-}"
vc ETL_LOGS_BUCKET "$ETL_LOGS_BUCKET"
vc ETL_SUCCESS_PREFIX "$ETL_SUCCESS_PREFIX"
echo "Done."
