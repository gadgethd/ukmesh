#!/usr/bin/env bash
# Backend/worker for owned lookup and ingest fixtures; normal DEV ports stay untouched.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOOKUP_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOOKUP_ENV_CLEAN=1 bash "$0" "$@"
fi
state="$repo/plans/database-lookups"
mkdir -p "$state"
stop_processes() {
  for service in backend region-snapshot; do
    local file="$state/$service.pid"
    if [[ -f $file ]]; then
      local pid
      pid=$(cat "$file")
      if [[ $pid =~ ^[0-9]+$ ]] && ps -p "$pid" -o args= | rg -q -- '--ukmesh-lookup-benchmark'; then
        kill -TERM "$pid"
        for ((attempt=0; attempt<60; attempt++)); do
          kill -0 "$pid" 2>/dev/null || break
          [[ $(ps -p "$pid" -o stat=) != Z* ]] || break
          sleep 1
        done
        if kill -0 "$pid" 2>/dev/null && [[ $(ps -p "$pid" -o stat=) != Z* ]]; then
          echo "$service did not shut down" >&2; exit 1
        fi
      fi
      rm -f "$file"
    fi
  done
}
case ${1:-help} in
  services)
    docker start ukmesh-lookup-db >/dev/null
    if docker inspect ukmesh-lookup-redis >/dev/null 2>&1; then docker start ukmesh-lookup-redis >/dev/null; else
      docker run -d --name ukmesh-lookup-redis --cpus 1 --memory 256m -p 127.0.0.1:6381:6379 \
        redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065 redis-server --save '' --appendonly no
    fi
    if docker inspect ukmesh-lookup-mqtt >/dev/null 2>&1; then docker start ukmesh-lookup-mqtt >/dev/null; else
      docker run -d --name ukmesh-lookup-mqtt --cpus 1 --memory 256m \
        -p 127.0.0.1:1885:1883 -p 127.0.0.1:9003:9001 \
        -v "$repo/scripts/local-sample/mosquitto-test.conf:/mosquitto/config/mosquitto.conf:ro" \
        eclipse-mosquitto@sha256:75d6662dee52c947dc9dd9c29f1d8d7ffef78ddaf38001caa09755681e8bbc98
    fi
    exists=$(docker exec ukmesh-lookup-db psql -X -U meshcore -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='lookup_owner_auth'")
    if [[ $exists == 0 ]]; then
      docker exec ukmesh-lookup-db createdb -U meshcore lookup_owner_auth
      docker exec -i ukmesh-lookup-db psql -X -U meshcore -d lookup_owner_auth -v ON_ERROR_STOP=1 \
        < "$repo/backend/src/db/owner-auth.sql" > "$state/owner-schema.log"
    fi
    ;;
  stop) stop_processes ;;
  start)
    name=${2:-lookup_10x}; kind=${3:-candidate}
    [[ $name =~ ^lookup_(1x|10x|soak|ingest_baseline|ingest_candidate)$ ]] || exit 2
    [[ $kind == candidate || $kind == baseline ]] || exit 2
    stop_processes
    bash "$0" services
    for service in ukmesh-lookup-db ukmesh-lookup-redis ukmesh-lookup-mqtt; do docker start "$service" >/dev/null; done
    source_root="$repo/backend"
    if [[ $kind == baseline ]]; then source_root="$state/baseline-source/backend"; fi
    [[ -f "$source_root/dist/index.js" ]] || { echo "Build $source_root first" >&2; exit 1; }
    lookup_jwt=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')
    lookup_cookie=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(32).toString("hex"))')
    lookup_operator=$(node -e 'process.stdout.write(require("node:crypto").randomBytes(24).toString("hex"))')
    local_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8
      DATABASE_URL="postgresql://meshcore:meshcore@127.0.0.1:5434/$name"
      MESHCORE_HTTP_DATABASE_URL="postgresql://meshcore:meshcore@127.0.0.1:5434/$name"
      MESHCORE_INGEST_DATABASE_URL="postgresql://meshcore:meshcore@127.0.0.1:5434/$name"
      MESHCORE_WORKER_DATABASE_URL="postgresql://meshcore:meshcore@127.0.0.1:5434/$name"
      OWNER_DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5434/lookup_owner_auth
      REDIS_URL=redis://127.0.0.1:6381 MQTT_BROKER_URL=ws://127.0.0.1:9003
      HTTP_HOST=127.0.0.1 METRICS_HOST=127.0.0.1 PORT=3101 METRICS_PORT=9093
      JWT_SECRET="$lookup_jwt" OWNER_COOKIE_SECRET="$lookup_cookie" OPERATOR_SITE_TOKEN="$lookup_operator"
      ALLOWED_ORIGINS=http://127.0.0.1:5173,http://localhost:5173
      OWNER_ACL_MODE=shadow MOSQUITTO_ACL_PATH="$repo/scripts/local-sample/mosquitto-acl"
      OWNER_ACL_SHADOW_PATH="$state/owner-acl.shadow" MOSQUITTO_LOG_PATH="$state/mosquitto.log"
      PATH_BETA_PRERESOLVE_ENABLED=0 DATA_LIFECYCLE_RETENTION_ENABLED=false PUBLIC_FEED_PUBLISH_ENABLED=false
      MQTT_CLIENT_ID=ukmesh-lookup-benchmark MQTT_INGEST_SHARE_GROUP=ukmesh-lookup-benchmark)
    touch "$state/mosquitto.log"
    setsid nohup "${local_env[@]}" node "$source_root/dist/index.js" --ukmesh-lookup-benchmark \
      > "$state/backend.log" 2>&1 < /dev/null &
    echo $! > "$state/backend.pid"
    ready=false
    for ((attempt=0; attempt<120; attempt++)); do
      kill -0 "$(cat "$state/backend.pid")" 2>/dev/null || break
      if curl --fail --silent --max-time 2 http://127.0.0.1:3101/healthz >/dev/null; then ready=true; break; fi
      sleep 1
    done
    [[ $ready == true ]] || { tail -25 "$state/backend.log" >&2; exit 1; }
    setsid nohup "${local_env[@]}" METRICS_PORT=9094 node "$source_root/dist/workers/region-snapshot.js" --ukmesh-lookup-benchmark \
      > "$state/region-snapshot.log" 2>&1 < /dev/null &
    echo $! > "$state/region-snapshot.pid"
    echo "Owned $kind backend: http://127.0.0.1:3101 ($name)"
    ;;
  *) echo 'Usage: scripts/lookup-stack-local.sh services | start lookup_10x|lookup_ingest_baseline|lookup_ingest_candidate candidate|baseline | stop' ;;
esac
