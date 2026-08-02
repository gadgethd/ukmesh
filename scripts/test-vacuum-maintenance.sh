#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
tmp_dir="$(mktemp -d)"
project_name="meshcore-vacuum-fixture-$$"
compose_file="$tmp_dir/compose.yml"
checkpoint_dir="$tmp_dir/checkpoints"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    COMPOSE_FILE="$compose_file" COMPOSE_PROJECT_NAME="$project_name" \
      docker compose logs --no-color timescaledb >&2 || true
  fi
  COMPOSE_FILE="$compose_file" COMPOSE_PROJECT_NAME="$project_name" \
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT

cat >"$compose_file" <<'YAML'
services:
  timescaledb:
    image: timescale/timescaledb@sha256:22e8a5ae7aef121d1537afe946dd7cc5deeeb63ab36ce19849d671bd3b663509
    environment:
      POSTGRES_DB: meshcore
      POSTGRES_USER: meshcore
      POSTGRES_PASSWORD: fixture-password
YAML

export COMPOSE_FILE="$compose_file"
export COMPOSE_PROJECT_NAME="$project_name"
docker compose up -d timescaledb >/dev/null
for _ in $(seq 1 60); do
  if docker compose logs --no-color timescaledb 2>/dev/null \
      | grep -q 'PostgreSQL init process complete' \
    && docker compose exec -T timescaledb pg_isready -U meshcore -d meshcore >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
docker compose logs --no-color timescaledb | grep -q 'PostgreSQL init process complete'
docker compose exec -T timescaledb pg_isready -U meshcore -d meshcore >/dev/null

docker compose exec -T timescaledb psql -X -v ON_ERROR_STOP=1 -U meshcore -d meshcore <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE maintenance_fixture (
  ts timestamptz NOT NULL,
  value integer NOT NULL
);
SELECT create_hypertable(
  'maintenance_fixture',
  by_range('ts', INTERVAL '1 day'),
  if_not_exists => TRUE
);
INSERT INTO maintenance_fixture
SELECT NOW() - INTERVAL '3 days' + make_interval(secs => value), value
FROM generate_series(1, 100) value;
ALTER TABLE maintenance_fixture SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts'
);
SELECT compress_chunk(chunk)
FROM show_chunks('maintenance_fixture', older_than => INTERVAL '1 day') chunk;
SQL

chunk="$(
  docker compose exec -T timescaledb psql -X -At -U meshcore -d meshcore -c \
    "SELECT format('%I.%I', chunk_schema, chunk_name)
       FROM timescaledb_information.chunks
      WHERE hypertable_name = 'maintenance_fixture' AND is_compressed
      ORDER BY range_start
      LIMIT 1"
)"
test -n "$chunk"

private_key="$tmp_dir/receipt-signing.pem"
verify_key="$tmp_dir/receipt-verify.pem"
receipt="$tmp_dir/restore-receipt.json"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$private_key" >/dev/null 2>&1
openssl pkey -in "$private_key" -pubout -out "$verify_key" >/dev/null
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg receipt_id fixture-restore-receipt \
  --arg backup_completed_at "$now" \
  --arg restore_verified_at "$now" \
  '{
    format: "meshcore-restore-receipt-v1",
    receipt_id: $receipt_id,
    status: "verified",
    backup_completed_at: $backup_completed_at,
    restore_verified_at: $restore_verified_at,
    datasets: ["analytics", "owner_auth", "mosquitto", "redis", "configuration"]
  }' >"$receipt"
openssl dgst -sha256 -sign "$private_key" -out "${receipt}.sig" "$receipt"

export MAINTENANCE_CHECKPOINT_DIR="$checkpoint_dir"
export RESTORE_RECEIPT_PATH="$receipt"
export RESTORE_RECEIPT_SIGNATURE="${receipt}.sig"
export RESTORE_RECEIPT_VERIFY_KEY="$verify_key"
export MAINTENANCE_LOCK_TIMEOUT=2s
export MAINTENANCE_STATEMENT_TIMEOUT=2min

"$project_dir/vacuum-compressed-chunks.sh" \
  --database=meshcore --chunk="$chunk" --action=vacuum
"$project_dir/vacuum-compressed-chunks.sh" \
  --database=meshcore --chunk="$chunk" --action=vacuum \
  --apply --approve="maintain-vacuum-meshcore-${chunk}" --dba-review=FIXTURE-REVIEW-1
"$project_dir/vacuum-compressed-chunks.sh" \
  --database=meshcore --chunk="$chunk" --action=vacuum \
  --apply --approve="maintain-vacuum-meshcore-${chunk}" --dba-review=FIXTURE-REVIEW-1

checkpoint="$checkpoint_dir/meshcore-${chunk//./-}-vacuum.json"
test -f "$checkpoint"
jq -e '.status == "complete" and .restore_receipt == "fixture-restore-receipt"' \
  "$checkpoint" >/dev/null
echo "isolated compressed-chunk dry-run, signed apply, and safe repeat passed"
