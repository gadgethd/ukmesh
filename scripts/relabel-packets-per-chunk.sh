#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

PSQL=(docker compose exec -T timescaledb psql -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -v ON_ERROR_STOP=1 -X)
# `test` remains an isolated network. This maintenance operation only folds
# historical production labels into the unified UKMesh scope.
LEGACY_SQL="'teesside','northeast'"

scalar() {
  "${PSQL[@]}" -qAt -c "$1" | tr -d '[:space:]'
}

disk_free() {
  df -h / | awk 'NR==2 {print $4 " free (" $5 " used)"}'
}

# Preserve the prior policy state. A relabel can take a long time, so an
# interrupted run must not accidentally leave compression disabled.
compression_schedule_before=''
compression_schedule_restore=0

restore_compression_schedule() {
  if [[ "$compression_schedule_restore" -ne 1 ]]; then
    return 0
  fi

  log "restoring packets compression policy (scheduled=${compression_schedule_before})"
  if ! "${PSQL[@]}" -qAt <<SQL
SELECT alter_job(j.job_id, scheduled => ${compression_schedule_before})
FROM timescaledb_information.jobs j
WHERE j.proc_name = 'policy_compression' AND j.hypertable_name = 'packets';
SQL
  then
    log "ERROR: could not restore the packets compression policy; restore it manually"
    return 1
  fi
  compression_schedule_restore=0
}

on_exit() {
  local status=$?
  trap - EXIT
  if ! restore_compression_schedule && [[ "$status" -eq 0 ]]; then
    status=1
  fi
  exit "$status"
}

log "packets per-chunk relabel starting"
log "disk before: $(disk_free)"

compression_schedule_before="$(scalar "SELECT CASE WHEN scheduled THEN 'true' ELSE 'false' END FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression' AND hypertable_name = 'packets' LIMIT 1;")"
case "$compression_schedule_before" in
  true)
    compression_schedule_restore=1
    trap on_exit EXIT
    log "pausing packets compression policy"
    "${PSQL[@]}" -qAt <<'SQL'
SELECT alter_job(j.job_id, scheduled => false)
FROM timescaledb_information.jobs j
WHERE j.proc_name = 'policy_compression' AND j.hypertable_name = 'packets';
SQL
    ;;
  false)
    log "packets compression policy is already disabled; leaving it disabled"
    ;;
  '')
    log "no packets compression policy found; continuing without changing scheduling"
    ;;
  *)
    log "unexpected packets compression schedule state: ${compression_schedule_before}"
    exit 1
    ;;
esac

remaining="$(scalar "SELECT count(*) FROM packets WHERE network IN (${LEGACY_SQL});")"
log "initial remaining legacy rows=${remaining}"

mapfile -t chunks < <("${PSQL[@]}" -qAt -F $'\t' <<'SQL'
SELECT chunk_schema, chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'packets'
ORDER BY range_start;
SQL
)

idx=0
for row in "${chunks[@]}"; do
  idx=$((idx + 1))
  IFS=$'\t' read -r schema chunk was_compressed <<< "$row"

  if [[ ! "$schema" =~ ^[A-Za-z0-9_]+$ || ! "$chunk" =~ ^[A-Za-z0-9_]+$ ]]; then
    log "unsafe chunk identifier: ${schema}.${chunk}"
    exit 1
  fi

  qual="${schema}.${chunk}"
  n="$(scalar "SELECT count(*) FROM ${qual} WHERE network IN (${LEGACY_SQL});")"

  if [[ "$n" == "0" ]]; then
    log "chunk ${idx}/${#chunks[@]} ${qual}: no legacy rows; skipped"
    continue
  fi

  log "chunk ${idx}/${#chunks[@]} ${qual}: ${n} legacy rows, compressed=${was_compressed}; updating"
  "${PSQL[@]}" <<SQL
\\timing on
BEGIN;
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;
UPDATE ${qual} SET network = 'ukmesh' WHERE network IN (${LEGACY_SQL});
COMMIT;
SQL

  if [[ "$was_compressed" == "t" ]]; then
    log "chunk ${idx}/${#chunks[@]} ${qual}: recompressing changed compressed batches"
    "${PSQL[@]}" <<SQL
\\timing on
CALL recompress_chunk('${qual}'::regclass, true);
SQL
  fi

  "${PSQL[@]}" -qAt -c "CHECKPOINT;" >/dev/null || log "checkpoint failed after ${qual}; continuing"
  remaining=$((remaining - n))
  log "chunk ${idx}/${#chunks[@]} ${qual}: done; estimated remaining legacy rows=${remaining}; disk=$(disk_free)"
done

log "final recompress pass for compressed chunks"
for row in "${chunks[@]}"; do
  IFS=$'\t' read -r schema chunk was_compressed <<< "$row"
  qual="${schema}.${chunk}"
  if [[ "$was_compressed" == "t" ]]; then
    "${PSQL[@]}" -qAt -c "CALL recompress_chunk('${qual}'::regclass, true);" >/dev/null \
      || log "recompress skipped/failed for ${qual}"
  fi
done

restore_compression_schedule

"${PSQL[@]}" -qAt -c "CHECKPOINT;" >/dev/null || true

log "exact packet network counts follow"
"${PSQL[@]}" <<'SQL'
SELECT network, count(*) FROM packets GROUP BY network ORDER BY network;
SELECT is_compressed, count(*) AS chunks
FROM timescaledb_information.chunks
WHERE hypertable_name='packets'
GROUP BY is_compressed
ORDER BY is_compressed;
SQL

log "disk after: $(disk_free)"
