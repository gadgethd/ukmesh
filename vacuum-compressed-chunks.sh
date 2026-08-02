#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
database="meshcore"
chunk=""
action="vacuum"
apply="false"
approval=""
dba_review=""
lock_timeout="${MAINTENANCE_LOCK_TIMEOUT:-5s}"
statement_timeout="${MAINTENANCE_STATEMENT_TIMEOUT:-30min}"
checkpoint_dir="${MAINTENANCE_CHECKPOINT_DIR:-/home/ben/meshcore-maintenance}"
receipt_path="${RESTORE_RECEIPT_PATH:-}"
receipt_signature="${RESTORE_RECEIPT_SIGNATURE:-${receipt_path:+${receipt_path}.sig}}"
receipt_verify_key="${RESTORE_RECEIPT_VERIFY_KEY:-}"

usage() {
  cat <<'EOF'
Usage:
  vacuum-compressed-chunks.sh
  vacuum-compressed-chunks.sh --database=DB --chunk=SCHEMA.TABLE [--action=vacuum|reindex|vacuum-full]
  vacuum-compressed-chunks.sh --database=DB --chunk=SCHEMA.TABLE --action=ACTION \
    --apply --approve=maintain-ACTION-DB-SCHEMA.TABLE --dba-review=REFERENCE

Without --apply the command is read-only. It inventories compressed chunks or
prints the exact single-chunk command, backup gate, timeout, and disk preflight.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --database=*) database="${arg#*=}" ;;
    --chunk=*) chunk="${arg#*=}" ;;
    --action=*) action="${arg#*=}" ;;
    --approve=*) approval="${arg#*=}" ;;
    --dba-review=*) dba_review="${arg#*=}" ;;
    --apply) apply="true" ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; usage >&2; exit 64 ;;
  esac
done

case "$database" in
  ''|*[!a-zA-Z0-9_]*)
    echo "database must be an exact PostgreSQL identifier" >&2
    exit 64
    ;;
esac
case "$action" in
  vacuum|reindex|vacuum-full) ;;
  *) echo "unsupported action: $action" >&2; exit 64 ;;
esac
if [ -n "$chunk" ]; then
  if [[ ! "$chunk" =~ ^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "chunk must be an exact SCHEMA.TABLE identifier" >&2
    exit 64
  fi
fi

cd "$repo_dir"
docker compose config -q
psql_command=(docker compose exec -T timescaledb psql -X -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-meshcore}" -d "$database")

database_exists="$("${psql_command[@]}" -Atc \
  "SELECT 1 FROM pg_database WHERE datname = current_database() AND NOT datistemplate")"
test "$database_exists" = "1" || {
  echo "database does not exist or is a template: $database" >&2
  exit 65
}

if [ -z "$chunk" ]; then
  echo "Read-only compressed chunk inventory for database ${database}:"
  "${psql_command[@]}" -P pager=off -c "
    SELECT
      format('%I.%I', chunk_schema, chunk_name) AS chunk,
      hypertable_name,
      pg_size_pretty(
        pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass)
      ) AS total_size,
      COALESCE(s.n_dead_tup, 0) AS estimated_dead_rows,
      s.last_vacuum,
      s.last_autovacuum
    FROM timescaledb_information.chunks c
    LEFT JOIN pg_stat_all_tables s
      ON s.schemaname = c.chunk_schema
     AND s.relname = c.chunk_name
    WHERE c.is_compressed = true
    ORDER BY pg_total_relation_size(
      format('%I.%I', chunk_schema, chunk_name)::regclass
    ) DESC"
  echo "No maintenance was executed. Select exactly one measured chunk to continue."
  exit 0
fi

schema="${chunk%%.*}"
table="${chunk#*.}"
chunk_row="$("${psql_command[@]}" -AtF '|' -c "
  SELECT
    pg_total_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass),
    COALESCE(s.n_dead_tup, 0)
  FROM timescaledb_information.chunks c
  LEFT JOIN pg_stat_all_tables s
    ON s.schemaname = c.chunk_schema
   AND s.relname = c.chunk_name
  WHERE c.is_compressed = true
    AND c.chunk_schema = '$schema'
    AND c.chunk_name = '$table'")"
test -n "$chunk_row" || {
  echo "selected relation is not a compressed Timescale chunk: $chunk" >&2
  exit 65
}
relation_bytes="${chunk_row%%|*}"
dead_rows="${chunk_row#*|}"

case "$action" in
  vacuum)
    sql="VACUUM (ANALYZE, VERBOSE) \"${schema}\".\"${table}\";"
    required_free_bytes="$((relation_bytes / 10 + 67108864))"
    ;;
  reindex)
    sql="REINDEX TABLE CONCURRENTLY \"${schema}\".\"${table}\";"
    required_free_bytes="$((relation_bytes * 2 + 67108864))"
    ;;
  vacuum-full)
    sql="VACUUM (FULL, ANALYZE, VERBOSE) \"${schema}\".\"${table}\";"
    required_free_bytes="$((relation_bytes * 3 + 67108864))"
    ;;
esac
available_bytes="$(
  docker compose exec -T timescaledb \
    df -PB1 /var/lib/postgresql/data | awk 'NR==2 {print $4}'
)"

echo "Maintenance plan (read-only until --apply):"
jq -n \
  --arg database "$database" \
  --arg chunk "$chunk" \
  --arg action "$action" \
  --arg sql "$sql" \
  --arg lock_timeout "$lock_timeout" \
  --arg statement_timeout "$statement_timeout" \
  --argjson relation_bytes "$relation_bytes" \
  --argjson estimated_dead_rows "$dead_rows" \
  --argjson required_free_bytes "$required_free_bytes" \
  --argjson available_bytes "$available_bytes" \
  '{
    database: $database,
    chunk: $chunk,
    action: $action,
    sql: $sql,
    lock_timeout: $lock_timeout,
    statement_timeout: $statement_timeout,
    relation_bytes: $relation_bytes,
    estimated_dead_rows: $estimated_dead_rows,
    required_free_bytes: $required_free_bytes,
    available_bytes: $available_bytes
  }'

if [ "$apply" != "true" ]; then
  echo "Dry run only; no maintenance was executed."
  exit 0
fi
if [ "$available_bytes" -lt "$required_free_bytes" ]; then
  echo "insufficient database-volume free space for selected action" >&2
  exit 75
fi
if [ "${#dba_review}" -lt 8 ]; then
  echo "--dba-review must name the approved ticket or review record" >&2
  exit 64
fi
expected_approval="maintain-${action}-${database}-${chunk}"
if [ "$approval" != "$expected_approval" ]; then
  echo "--approve=${expected_approval} is required" >&2
  exit 64
fi
if [ "$action" = "vacuum-full" ] && [ "${ALLOW_VACUUM_FULL:-false}" != "true" ]; then
  echo "VACUUM FULL is exceptional; ALLOW_VACUUM_FULL=true is also required" >&2
  exit 64
fi

for path in "$receipt_path" "$receipt_signature" "$receipt_verify_key"; do
  test -r "$path" || {
    echo "signed restore receipt input is missing: ${path:-unset}" >&2
    exit 66
  }
done
openssl dgst -sha256 -verify "$receipt_verify_key" \
  -signature "$receipt_signature" "$receipt_path" >/dev/null
test "$(jq -r '.format' "$receipt_path")" = "meshcore-restore-receipt-v1"
test "$(jq -r '.status' "$receipt_path")" = "verified"
for dataset in analytics owner_auth mosquitto redis configuration; do
  jq -e --arg dataset "$dataset" '.datasets | index($dataset) != null' \
    "$receipt_path" >/dev/null
done
now_epoch="$(date +%s)"
backup_epoch="$(date -d "$(jq -r '.backup_completed_at' "$receipt_path")" +%s)"
restore_epoch="$(date -d "$(jq -r '.restore_verified_at' "$receipt_path")" +%s)"
maximum_age_seconds="$((7 * 24 * 60 * 60))"
if [ "$((now_epoch - backup_epoch))" -gt "$maximum_age_seconds" ] \
  || [ "$((now_epoch - restore_epoch))" -gt "$maximum_age_seconds" ] \
  || [ "$backup_epoch" -gt "$((now_epoch + 300))" ] \
  || [ "$restore_epoch" -gt "$((now_epoch + 300))" ]; then
  echo "backup or restore verification is stale or future-dated" >&2
  exit 65
fi

mkdir -p "$checkpoint_dir"
checkpoint="$checkpoint_dir/${database}-${schema}-${table}-${action}.json"
lock_file="${checkpoint}.lock"
exec 9>"$lock_file"
flock -n 9 || {
  echo "the selected maintenance action is already running" >&2
  exit 75
}
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
  --arg status running \
  --arg database "$database" \
  --arg chunk "$chunk" \
  --arg action "$action" \
  --arg dba_review "$dba_review" \
  --arg started_at "$started_at" \
  --arg receipt_id "$(jq -r '.receipt_id' "$receipt_path")" \
  '{status:$status,database:$database,chunk:$chunk,action:$action,dba_review:$dba_review,started_at:$started_at,restore_receipt:$receipt_id}' \
  >"${checkpoint}.partial"
mv "${checkpoint}.partial" "$checkpoint"

set +e
"${psql_command[@]}" <<SQL
\set ON_ERROR_STOP on
SET lock_timeout = '${lock_timeout}';
SET statement_timeout = '${statement_timeout}';
${sql}
SQL
maintenance_status=$?
set -e
completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
status="complete"
if [ "$maintenance_status" -ne 0 ]; then
  status="failed"
fi
jq \
  --arg status "$status" \
  --arg completed_at "$completed_at" \
  '. + {status:$status,completed_at:$completed_at}' \
  "$checkpoint" >"${checkpoint}.partial"
mv "${checkpoint}.partial" "$checkpoint"

if [ "$maintenance_status" -ne 0 ]; then
  echo "maintenance aborted safely; checkpoint retained at $checkpoint" >&2
  exit "$maintenance_status"
fi
echo "single-chunk maintenance completed; checkpoint: $checkpoint"
