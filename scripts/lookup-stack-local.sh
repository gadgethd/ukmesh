#!/usr/bin/env bash
# Backend/worker for owned lookup and ingest fixtures; normal DEV ports stay untouched.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOOKUP_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOOKUP_ENV_CLEAN=1 bash "$0" "$@"
fi
source "$repo/scripts/lib/owned-processes.sh"
source "$repo/scripts/local-sample/lookup-profile.sh"
state="$repo/plans/database-lookups"
mkdir -p "$state"
chmod 700 "$state"
lookup_entrypoint() {
  local service=$1 source_root=$2
  case "$service" in
    backend) printf '%s/dist/index.js\n' "$source_root" ;;
    region-snapshot) printf '%s/dist/workers/region-snapshot.js\n' "$source_root" ;;
  esac
}

stop_processes() {
  local service source_root entrypoint
  for service in backend region-snapshot; do
    # An old PID can belong to either previously selected source revision.
    for source_root in "$repo/backend" "$state/baseline-source/backend"; do
      entrypoint=$(lookup_entrypoint "$service" "$source_root")
      if [[ -f "$state/$service.pid" ]] && owned_process_matches "$(cat "$state/$service.pid")" "$entrypoint" --ukmesh-lookup-benchmark; then
        owned_stop_process "$state/$service.pid" "$entrypoint" 60 1 "$service" --ukmesh-lookup-benchmark || return
      fi
    done
    rm -f -- "$state/$service.pid"
  done
}

lookup_database_ready() {
  docker exec "$LOOKUP_DB_CONTAINER" pg_isready -h 127.0.0.1 -U meshcore -d postgres >/dev/null 2>&1
}

start_services() {
  docker start "$LOOKUP_DB_CONTAINER" >/dev/null
  if ! owned_wait_until 120 1 lookup_database_ready; then
    echo 'Owned lookup database did not become ready' >&2
    return 1
  fi
  if docker inspect "$LOOKUP_REDIS_CONTAINER" >/dev/null 2>&1; then docker start "$LOOKUP_REDIS_CONTAINER" >/dev/null; else
    docker run -d --name "$LOOKUP_REDIS_CONTAINER" --cpus 1 --memory 256m -p 127.0.0.1:$LOOKUP_REDIS_PORT:6379 \
      redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065 redis-server --save '' --appendonly no
  fi
  if docker inspect "$LOOKUP_MQTT_CONTAINER" >/dev/null 2>&1; then docker start "$LOOKUP_MQTT_CONTAINER" >/dev/null; else
    docker run -d --name "$LOOKUP_MQTT_CONTAINER" --cpus 1 --memory 256m \
      -p 127.0.0.1:$LOOKUP_MQTT_TCP_PORT:1883 -p 127.0.0.1:$LOOKUP_MQTT_WS_PORT:9001 \
      -v "$repo/scripts/local-sample/mosquitto-test.conf:/mosquitto/config/mosquitto.conf:ro" \
      eclipse-mosquitto@sha256:75d6662dee52c947dc9dd9c29f1d8d7ffef78ddaf38001caa09755681e8bbc98
  fi
  exists=$(docker exec "$LOOKUP_DB_CONTAINER" psql -X -U meshcore -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='lookup_owner_auth'")
  if [[ $exists == 0 ]]; then
    docker exec "$LOOKUP_DB_CONTAINER" createdb -U meshcore lookup_owner_auth
    docker exec -i "$LOOKUP_DB_CONTAINER" psql -X -U meshcore -d lookup_owner_auth -v ON_ERROR_STOP=1 \
      < "$repo/backend/src/db/owner-auth.sql" > "$state/owner-schema.log"
  fi
}

case ${1:-help} in
  services) start_services ;;
  stop) stop_processes ;;
  start)
    name=${2:-lookup_10x}; kind=${3:-candidate}
    lookup_database_name "$name" || exit 2
    [[ $kind == candidate || $kind == baseline ]] || exit 2
    stop_processes
    start_services
    source_root="$repo/backend"
    if [[ $kind == baseline ]]; then source_root="$state/baseline-source/backend"; fi
    [[ -f "$source_root/dist/index.js" ]] || { echo "Build $source_root first" >&2; exit 1; }
    launched_pids=()
    launched_services=()
    unwind_start() {
      local result=$? index service process_id entrypoint
      if (( result != 0 )); then
        for ((index=${#launched_pids[@]}-1; index>=0; index--)); do
          service=${launched_services[index]}; process_id=${launched_pids[index]}
          entrypoint=$(lookup_entrypoint "$service" "$source_root")
          if owned_stop_pid "$process_id" "$entrypoint" 60 1 "$service" --ukmesh-lookup-benchmark; then
            if [[ -f "$state/$service.pid" && $(cat "$state/$service.pid") == "$process_id" ]]; then rm -f -- "$state/$service.pid"; fi
          fi
        done
      fi
      return "$result"
    }
    trap unwind_start EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    lookup_jwt=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')
    lookup_cookie=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')
    lookup_operator=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(24).toString("hex"))')
    local_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8
      DATABASE_URL="$LOOKUP_DATABASE_BASE/$name"
      MESHCORE_HTTP_DATABASE_URL="$LOOKUP_DATABASE_BASE/$name"
      MESHCORE_INGEST_DATABASE_URL="$LOOKUP_DATABASE_BASE/$name"
      MESHCORE_WORKER_DATABASE_URL="$LOOKUP_DATABASE_BASE/$name"
      OWNER_DATABASE_URL=$LOOKUP_DATABASE_BASE/lookup_owner_auth
      REDIS_URL="$LOOKUP_REDIS_URL" MQTT_BROKER_URL="$LOOKUP_BROKER_URL"
      HTTP_HOST=127.0.0.1 METRICS_HOST=127.0.0.1 PORT="$LOOKUP_HTTP_PORT" METRICS_PORT="$LOOKUP_METRICS_PORT"
      JWT_SECRET="$lookup_jwt" OWNER_COOKIE_SECRET="$lookup_cookie" OPERATOR_SITE_TOKEN="$lookup_operator"
      ALLOWED_ORIGINS=http://127.0.0.1:5173,http://localhost:5173
      OWNER_ACL_MODE=shadow MOSQUITTO_ACL_PATH="$repo/scripts/local-sample/mosquitto-acl"
      OWNER_ACL_SHADOW_PATH="$state/owner-acl.shadow" MOSQUITTO_LOG_PATH="$state/mosquitto.log"
      PATH_BETA_PRERESOLVE_ENABLED=0 DATA_LIFECYCLE_RETENTION_ENABLED=false PUBLIC_FEED_PUBLISH_ENABLED=false
      MQTT_CLIENT_ID=ukmesh-lookup-benchmark MQTT_INGEST_SHARE_GROUP=ukmesh-lookup-benchmark)
    touch "$state/mosquitto.log"
    setsid nohup "${local_env[@]}" node "$source_root/dist/index.js" --ukmesh-lookup-benchmark \
      > "$state/backend.log" 2>&1 < /dev/null &
    launched_pids+=("$!"); launched_services+=(backend)
    printf '%s\n' "${launched_pids[-1]}" > "$state/backend.pid"
    ready=false
    for ((attempt=0; attempt<120; attempt++)); do
      owned_process_alive "${launched_pids[0]}" || break
      if curl --fail --silent --max-time 2 "$LOOKUP_HTTP_URL/healthz" >/dev/null; then ready=true; break; fi
      sleep 1
    done
    [[ $ready == true ]] || { tail -25 "$state/backend.log" >&2; exit 1; }
    setsid nohup "${local_env[@]}" METRICS_PORT="$LOOKUP_REGION_METRICS_PORT" node "$source_root/dist/workers/region-snapshot.js" --ukmesh-lookup-benchmark \
      > "$state/region-snapshot.log" 2>&1 < /dev/null &
    launched_pids+=("$!"); launched_services+=(region-snapshot)
    printf '%s\n' "${launched_pids[-1]}" > "$state/region-snapshot.pid"
    region_ready=false
    for ((attempt=0; attempt<180; attempt++)); do
      owned_process_alive "${launched_pids[-1]}" || break
      if curl --fail --silent --max-time 2 "http://127.0.0.1:$LOOKUP_REGION_METRICS_PORT/metrics" | grep '^meshcore_region_snapshot_healthy 1$' >/dev/null; then region_ready=true; break; fi
      sleep 1
    done
    [[ $region_ready == true ]] || { tail -30 "$state/region-snapshot.log" >&2; exit 1; }
    trap - EXIT INT TERM
    echo "Owned $kind backend: $LOOKUP_HTTP_URL ($name)"
    ;;
  *) echo 'Usage: scripts/lookup-stack-local.sh services | start lookup_10x|lookup_ingest_baseline|lookup_ingest_candidate candidate|baseline | stop' ;;
esac
