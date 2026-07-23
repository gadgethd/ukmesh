#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-audit}"
PSQL=(docker compose exec -T timescaledb psql -U "${POSTGRES_USER:-meshcore}" -d "${POSTGRES_DB:-meshcore}" -v ON_ERROR_STOP=1 -X)
LEGACY_SQL="'teesside','northeast'"
RUN_ID="${NETWORK_UNIFICATION_RUN_ID:-network-unification-$(date -u +%Y%m%dT%H%M%SZ)}"

if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  printf 'Unsafe NETWORK_UNIFICATION_RUN_ID: %s\n' "$RUN_ID" >&2
  exit 2
fi

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }
scalar() { "${PSQL[@]}" -qAt -c "$1" | tr -d '[:space:]'; }

audit() {
  log 'network-label counts'
  "${PSQL[@]}" <<'SQL'
SELECT 'nodes' AS relation, network, COUNT(*) FROM nodes GROUP BY network
UNION ALL SELECT 'node_status_samples', network, COUNT(*) FROM node_status_samples GROUP BY network
UNION ALL SELECT 'packets', network, COUNT(*) FROM packets GROUP BY network
UNION ALL SELECT 'node_network_sightings', network, COUNT(*) FROM node_network_sightings GROUP BY network
UNION ALL SELECT 'path_prefix_priors', network, COUNT(*) FROM path_prefix_priors GROUP BY network
ORDER BY 1, 2;
SQL
  log 'recent legacy-writer activity'
  "${PSQL[@]}" -c "SELECT network, MAX(time) AS latest, COUNT(*) AS packets_15m FROM packets WHERE network IN (${LEGACY_SQL}) AND time > NOW() - INTERVAL '15 minutes' GROUP BY network ORDER BY network;"
}

verify() {
  local remaining
  remaining="$(scalar "SELECT (SELECT COUNT(*) FROM nodes WHERE network IN (${LEGACY_SQL})) + (SELECT COUNT(*) FROM node_status_samples WHERE network IN (${LEGACY_SQL})) + (SELECT COUNT(*) FROM packets WHERE network IN (${LEGACY_SQL})) + (SELECT COUNT(*) FROM node_network_sightings WHERE network IN (${LEGACY_SQL}));")"
  audit
  if [[ "$remaining" != '0' ]]; then
    log "ERROR: ${remaining} authoritative rows still use legacy production labels"
    return 1
  fi
  log 'verification passed: authoritative production labels are unified'
}

relabel_status_batches() {
  local batch_size="${NETWORK_UNIFICATION_STATUS_BATCH_SIZE:-50000}"
  local changed=1 total=0
  while [[ "$changed" -gt 0 ]]; do
    changed="$(scalar "WITH batch AS MATERIALIZED (SELECT ctid FROM node_status_samples WHERE network IN (${LEGACY_SQL}) LIMIT ${batch_size}), changed AS (UPDATE node_status_samples target SET network = 'ukmesh' FROM batch WHERE target.ctid = batch.ctid RETURNING 1) SELECT COUNT(*) FROM changed;")"
    total=$((total + changed))
    log "node_status_samples relabelled=${total} last_batch=${changed}"
  done
}

mark_failed() {
  local status=$?
  trap - ERR
  "${PSQL[@]}" -qAt -c "UPDATE network_unification_runs SET status='failed', detail=jsonb_build_object('exit_status', ${status}) WHERE run_id='${RUN_ID//\'/\'\'}';" >/dev/null || true
  exit "$status"
}

apply() {
  if [[ "${CONFIRM_NETWORK_UNIFICATION:-}" != 'ukmesh' ]]; then
    log 'ERROR: set CONFIRM_NETWORK_UNIFICATION=ukmesh for the irreversible apply step'
    return 2
  fi
  if [[ -z "${BACKUP_REFERENCE:-}" ]]; then
    log 'ERROR: BACKUP_REFERENCE must identify a verified pre-cutover database snapshot'
    return 2
  fi
  local active_legacy
  active_legacy="$(scalar "SELECT COUNT(*) FROM packets WHERE network IN (${LEGACY_SQL}) AND time > NOW() - INTERVAL '15 minutes';")"
  if [[ "$active_legacy" != '0' ]]; then
    log "ERROR: ${active_legacy} legacy-labelled packets arrived in the last 15 minutes; update/stop legacy writers first"
    return 3
  fi

  trap mark_failed ERR
  "${PSQL[@]}" -qAt -c "INSERT INTO network_unification_runs (run_id,status,backup_reference) VALUES ('${RUN_ID//\'/\'\'}','running','${BACKUP_REFERENCE//\'/\'\'}') ON CONFLICT (run_id) DO UPDATE SET status='running';"
  audit
  log 'applying small-table and derived-table cutover'
  "${PSQL[@]}" -f scripts/unify-networks-migration.sql
  log 'relabeling node status history in bounded commits'
  relabel_status_batches
  log 'relabeling packet chunks with compression-state preservation'
  scripts/relabel-packets-per-chunk.sh
  verify
  "${PSQL[@]}" -qAt -c "UPDATE network_unification_runs SET status='completed', completed_at=NOW(), detail=jsonb_build_object('verified_at', NOW()) WHERE run_id='${RUN_ID//\'/\'\'}';"
  trap - ERR
  log "cutover complete run_id=${RUN_ID}"
}

case "$MODE" in
  audit) audit ;;
  verify) verify ;;
  apply) apply ;;
  *) printf 'Usage: %s [audit|apply|verify]\n' "$0" >&2; exit 2 ;;
esac
