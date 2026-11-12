#!/usr/bin/env bash
# All subprocesses receive an allowlisted local environment, never a project .env.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOCAL_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOCAL_ENV_CLEAN=1 \
    bash "$repo/scripts/dev-stack.sh" "$@"
fi
cd "$repo"
state="$repo/plans/dev-stack"
mkdir -p "$state"
chmod 700 "$state"
compose=(docker compose -f "$repo/scripts/local-sample/compose.yml")
action=${1:-up}
if (($#)); then shift; fi
data_dir= profile=ingest shift_to= redact_content=false replay_limit=20 synthetic_count=200 ingest_flush_ms=
while (($#)); do
  case "$1" in
    --data-dir) data_dir=$(realpath "$2"); shift 2 ;;
    --profile) profile=$2; shift 2 ;;
    --shift-to) shift_to=$2; shift 2 ;;
    --redact-content) redact_content=true; shift ;;
    --replay-limit) replay_limit=$2; shift 2 ;;
    --synthetic-count) synthetic_count=$2; shift 2 ;;
    --ingest-flush-ms) ingest_flush_ms=$2; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
case "$profile" in snapshot|ingest) ;; *) echo 'Profile must be snapshot or ingest' >&2; exit 2 ;; esac
[[ $replay_limit =~ ^[0-9]+$ && $replay_limit -le 1000 ]] || { echo '--replay-limit must be 0..1000' >&2; exit 2; }
[[ -z $ingest_flush_ms || ( $ingest_flush_ms =~ ^[0-9]+$ && $ingest_flush_ms -le 1000 ) ]] || { echo '--ingest-flush-ms must be 0..1000' >&2; exit 2; }
case "$action" in
  help|--help)
    echo 'scripts/dev-stack.sh [up|start|status|stop|reset|cleanup] [--profile snapshot|ingest] [--data-dir DIR] [--shift-to ISO] [--redact-content]'
    echo 'up refuses an existing fixture; reset explicitly recreates only this stack. stop retains data; cleanup deletes its volume.'
    exit 0 ;;
  up|start|status|stop|reset|cleanup) ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
command -v docker >/dev/null || { echo 'Docker Engine and Compose are required' >&2; exit 1; }
docker info >/dev/null

stop_processes() {
  for service in backend frontend; do
    if [[ -f "$state/$service.pid" ]]; then
      pid=$(cat "$state/$service.pid")
      # Match our absolute entrypoint before signalling a saved PID.
      if [[ -r /proc/$pid/cmdline ]] && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -Fq "$repo"; then
        kill "$pid" 2>/dev/null || true
        for ((attempt=0; attempt<100; attempt++)); do
          kill -0 "$pid" 2>/dev/null || break
          sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then echo "$service did not stop; inspect PID $pid" >&2; return 1; fi
      fi
      rm "$state/$service.pid"
    fi
  done
}
case "$action" in
  status) "${compose[@]}" ps; curl --fail --silent http://127.0.0.1:3100/healthz; exit ;;
  stop) stop_processes; "${compose[@]}" stop; exit ;;
  cleanup) stop_processes; "${compose[@]}" down --volumes; rm -f "$state/fixture-ready"; exit ;;
  reset) stop_processes; "${compose[@]}" down --volumes; rm -f "$state/fixture-ready" ;;
  start)
    [[ -f "$state/fixture-ready" ]] || { echo 'No prepared fixture; use up or reset' >&2; exit 1; }
    profile=$(cat "$state/fixture-ready")
    data_dir=$(cat "$state/data-dir")
    if [[ -z $ingest_flush_ms && -f "$state/ingest-flush-ms" ]]; then ingest_flush_ms=$(cat "$state/ingest-flush-ms"); fi
    stop_processes ;;
  up)
    if [[ -f "$state/fixture-ready" ]] || [[ -n $("${compose[@]}" ps -aq) ]]; then
      echo 'An owned stack already exists; use status, or reset to explicitly discard and rebuild it.' >&2; exit 1
    fi ;;
esac
command -v node >/dev/null
command -v npm >/dev/null
if [[ ! -d backend/node_modules ]]; then npm --prefix backend ci; fi
if [[ ! -d frontend/node_modules ]]; then npm --prefix frontend ci; fi
"${compose[@]}" up -d --wait --wait-timeout 180
# pg_isready over TCP plus a health check avoids the temporary init server.
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
export MESHCORE_HTTP_DATABASE_URL=$DATABASE_URL MESHCORE_INGEST_DATABASE_URL=$DATABASE_URL
export MESHCORE_WORKER_DATABASE_URL=$DATABASE_URL MESHCORE_MIGRATE_DATABASE_URL=$DATABASE_URL
export OWNER_DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore_owner_auth
export REDIS_URL=redis://127.0.0.1:6380 MQTT_BROKER_URL=ws://127.0.0.1:9002
export PORT=3100 JWT_SECRET=local-only-development-secret-at-least-32-characters
export HTTP_HOST=127.0.0.1 METRICS_HOST=127.0.0.1
export OWNER_COOKIE_SECRET=local-only-cookie-secret-at-least-32-characters OPERATOR_SITE_TOKEN=local-only-token
export OWNER_ACL_MODE=shadow MOSQUITTO_ACL_PATH="$repo/scripts/local-sample/mosquitto-acl"
export OWNER_ACL_SHADOW_PATH="$state/owner-acl.shadow" MOSQUITTO_LOG_PATH="$state/mosquitto.log"
export PATH_BETA_PRERESOLVE_ENABLED=0 DATA_LIFECYCLE_RETENTION_ENABLED=false
export PUBLIC_FEED_PUBLISH_ENABLED=false
export MQTT_CLIENT_ID=ukmesh-local-sample MQTT_INGEST_SHARE_GROUP=ukmesh-local-sample
if [[ -n $ingest_flush_ms ]]; then
  export MQTT_INGEST_FLUSH_MS=$ingest_flush_ms
  printf '%s\n' "$ingest_flush_ms" > "$state/ingest-flush-ms"
else
  rm -f "$state/ingest-flush-ms"
fi
export UKMESH_DEV_API_TARGET=http://127.0.0.1:3100
export VITE_BUILD_TARGET=public-website VITE_SITE=ukmesh VITE_APP_HOSTNAME=localhost
tsx=(node --import "$repo/backend/node_modules/tsx/dist/loader.mjs")
if [[ $action != start ]]; then
for bootstrap in bootstrap-backend-roles.sql bootstrap-reader-roles.sql; do
  "${compose[@]}" exec -T db psql -U meshcore -d meshcore -v ON_ERROR_STOP=1 \
    < "$repo/scripts/$bootstrap" > "$state/$bootstrap.log"
done
MIGRATION_BASE_SCHEMA_BOOTSTRAP_APPROVAL=bootstrap-ledger-from-base-schema \
  "${tsx[@]}" "$repo/backend/src/tools/migrate.ts" > "$state/migrate.log" 2>&1 || { tail -30 "$state/migrate.log" >&2; exit 1; }
"${compose[@]}" exec -T db psql -U meshcore -d meshcore -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'
"${compose[@]}" exec -T db createdb -U meshcore meshcore_owner_auth
"${compose[@]}" exec -T db psql -U meshcore -d meshcore_owner_auth -v ON_ERROR_STOP=1 \
  < "$repo/backend/src/db/owner-auth.sql" > "$state/owner-schema.log"
: > "$state/mosquitto.log"
if [[ -z $data_dir ]]; then
  data_dir=$(mktemp -d "$state/synthetic.XXXXXX")
  "${tsx[@]}" "$repo/backend/src/tools/generateSampleDataset.ts" --data-dir "$data_dir" --count "$synthetic_count"
fi
load_args=(--data-dir "$data_dir" --fast)
if [[ -n $shift_to ]]; then load_args+=(--shift-to "$shift_to"); fi
if [[ $redact_content == true ]]; then load_args+=(--redact-content); fi
if [[ $profile == ingest ]]; then load_args+=(--omit-packets); fi
"${tsx[@]}" "$repo/backend/src/tools/loadSampleDataset.ts" "${load_args[@]}"
if [[ $profile == snapshot ]]; then
  "${tsx[@]}" "$repo/backend/src/tools/refreshSampleRecent.ts"
fi
"${tsx[@]}" "$repo/backend/src/tools/prepareSampleDataset.ts" > "$state/prepare.log" 2>&1 || { tail -30 "$state/prepare.log" >&2; exit 1; }
"${tsx[@]}" "$repo/backend/src/tools/backfillStatsRollups.ts" --apply --pause-ms 1 \
  > "$state/rollups.log" 2>&1 || { tail -30 "$state/rollups.log" >&2; exit 1; }
has_scoped=$("${compose[@]}" exec -T db psql -U meshcore -d meshcore -tAc \
  'SELECT EXISTS (SELECT 1 FROM mesh.packets WHERE route_type IN (0, 3))')
if [[ $has_scoped == t ]]; then
  "${tsx[@]}" "$repo/backend/src/tools/rebuildRegionParticipation.ts" \
    > "$state/regions.log" 2>&1 || { tail -30 "$state/regions.log" >&2; exit 1; }
fi
printf '%s\n' "$profile" > "$state/fixture-ready"
printf '%s\n' "$data_dir" > "$state/data-dir"
fi
npm --prefix backend run build > "$state/build.log" 2>&1 || { tail -30 "$state/build.log" >&2; exit 1; }
setsid nohup node "$repo/backend/dist/index.js" \
  > "$state/backend.log" 2>&1 < /dev/null &
echo $! > "$state/backend.pid"
ready=false
for ((attempt=0; attempt<120; attempt++)); do
  if curl --fail --silent http://127.0.0.1:3100/healthz > /dev/null; then ready=true; break; fi
  kill -0 "$(cat "$state/backend.pid")" 2>/dev/null || break
  sleep 1
done
if [[ $ready != true ]]; then tail -40 "$state/backend.log" >&2; exit 1; fi
if [[ $profile == ingest && $action != start && $replay_limit -gt 0 ]]; then
  "${tsx[@]}" "$repo/backend/src/tools/smokeSampleReplay.ts" --data-dir "$data_dir" --limit "$replay_limit"
fi
(
  cd "$repo/frontend"
  setsid nohup node "$repo/frontend/node_modules/vite/bin/vite.js" --host 127.0.0.1 --port 5173 --strictPort \
    > "$state/frontend.log" 2>&1 < /dev/null &
  echo $! > "$state/frontend.pid"
)
for ((attempt=0; attempt<60; attempt++)); do
  if curl --fail --silent http://127.0.0.1:5173/ > /dev/null; then
    echo 'Local website: http://127.0.0.1:5173  API: http://127.0.0.1:3100'
    echo "Profile: $profile. Logs and fixture evidence: $state"
    exit 0
  fi
  sleep 1
done
tail -30 "$state/frontend.log" >&2
exit 1
