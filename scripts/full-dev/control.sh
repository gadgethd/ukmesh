#!/usr/bin/env bash
set -euo pipefail
export DOCKER_HOST=unix:///var/run/docker.sock
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)

# Acquire the target lease before the independent controller lock. The live
# owner frame and inherited FD are verified on every shell re-entry.
entry_env=(env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8)
for entry_key in UKMESH_TARGET_LEASE_FD UKMESH_TARGET_ENTRY; do
  [[ ! -v $entry_key ]] || entry_env+=("$entry_key=${!entry_key}")
done
if "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" verify "$repo/scripts/full-dev/control.sh" "$@"; then :; else
  entry_status=$?
  [[ $entry_status == 1 ]] || exit "$entry_status"
  exec "${entry_env[@]}" python3 "$repo/scripts/full-dev/qualification/target_entry.py" run "$repo/scripts/full-dev/control.sh" "$@"
fi
cd "$repo"
source "$repo/scripts/full-dev/runtime.sh"
sample_compose=(docker compose --project-name ukmesh-sample --env-file /dev/null \
  -f "$repo/scripts/local-sample/compose.yml")
source "$repo/scripts/lib/owned-processes.sh"
state="$repo/plans/lean-runtime/runtime"
mkdir -p "$state"
chmod 700 "$repo/plans/lean-runtime" "$state"
action=${1:-start}
service=${2:-}
case "$action" in
  start|build|status|stop|logs|restart|hot|config) ;;
  *) echo "Unknown full-stack action: $action" >&2; exit 2 ;;
esac
if [[ -n $service && $action != hot ]]; then
  "${compose[@]}" config --services | rg -Fx -- "$service" > /dev/null || { echo "Unknown service: $service" >&2; exit 2; }
fi
case "$action" in
  status) "${compose[@]}" ps -a; exit ;;
  logs) "${compose[@]}" logs --tail 100 ${service:+"$service"}; exit ;;
  config) "${compose[@]}" config --quiet; exit ;;
  stop)
    "${compose[@]}" stop
    bash "$repo/scripts/dev-stack.sh" stop
    exit ;;
  restart)
    [[ -n $service ]] || { echo 'restart requires a service name' >&2; exit 2; }
    python3 "$repo/scripts/full-dev/qualification/target_entry.py" guard main restart "$service"
    python3 "$repo/scripts/full-dev/wiring.py"
    refresh_wiring_revisions
    "${compose[@]}" up -d --no-deps --force-recreate "$service"; exit ;;
esac

prepare_wiring() {
  python3 "$repo/scripts/full-dev/wiring.py"
  refresh_wiring_revisions
}

build_sample_broker() {
  echo "Building sample MQTT broker (component sources only)"
  "${sample_compose[@]}" build mqtt > "$state/build-sample-mqtt.log" 2>&1 || {
    tail -40 "$state/build-sample-mqtt.log" >&2; return 1;
  }
}

build_images() {
  bash "$repo/scripts/healthcheck-source.sh" --verify >/dev/null
  prepare_wiring
  build_sample_broker
  for component in backend app-ukmesh website-ukmesh website-dev tagger-worker link-worker hopreach mesh-health-check mosquitto-reloader; do
    echo "Building $component (one image at a time)"
    "${compose_build[@]}" build "$component" > "$state/build-$component.log" 2>&1 || { tail -40 "$state/build-$component.log" >&2; return 1; }
  done
  git rev-parse HEAD > "$state/built-revision"
}

if [[ $action == build ]]; then build_images; exit; fi
if [[ $action == hot ]]; then
  [[ $service == ukmesh ]] || { echo 'hot supports ukmesh or beacon-web' >&2; exit 2; }
  "${compose[@]}" -f "$repo/scripts/full-dev/hot.yml" up -d --no-deps --force-recreate website-ukmesh
  exit
fi

prepare_wiring
if ! docker network inspect ukmesh-sample_default >/dev/null 2>&1; then
  if [[ -f $repo/plans/dev-stack/fixture-ready ]]; then bash "$repo/scripts/dev-stack.sh" start; else bash "$repo/scripts/dev-stack.sh" up; fi
fi
build_sample_broker
"${sample_compose[@]}" up -d --wait db redis mqtt
images_ready=true
for image in "$BACKEND_IMAGE" "$APP_IMAGE" "$WEBSITE_IMAGE" "$WEBSITE_DEV_IMAGE" "$TAGGER_WORKER_IMAGE" "$RF_WORKER_IMAGE" "$HOPREACH_IMAGE" "$HEALTHCHECK_IMAGE" "$MOSQUITTO_RELOADER_IMAGE"; do
  docker image inspect "$image" >/dev/null 2>&1 || images_ready=false
done
if [[ $images_ready != true ]]; then build_images; fi

python3 "$repo/scripts/full-dev/qualification/target_entry.py" guard main

# Schema owners run only in this bounded bootstrap step. Runtime containers
# below receive their role DSNs, including on a newly selected fixture.
"${compose[@]}" up -d --no-deps --force-recreate db-migrate > "$state/migrate.log" 2>&1 || { tail -30 "$state/migrate.log" >&2; exit 1; }
migration_wait=0
"${compose[@]}" wait db-migrate >> "$state/migrate.log" 2>&1 || migration_wait=$?
"${compose[@]}" logs --no-color db-migrate >> "$state/migrate.log" 2>&1
migration_container=$("${compose[@]}" ps -a -q db-migrate)
[[ -n $migration_container ]] || { echo 'Database migration container is missing' >&2; exit 1; }
migration_state=$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$migration_container")
[[ $migration_wait == 0 && $migration_state == exited:0 ]] || {
  tail -30 "$state/migrate.log" >&2
  echo 'Database migration job failed or did not complete' >&2; exit 1;
}
bash "$repo/scripts/full-dev/access.sh" "$UKMESH_DATABASE" "$OWNER_DATABASE" > "$state/access.log" 2>&1 || { tail -20 "$state/access.log" >&2; exit 1; }

# Retire only PIDs owned by the old fixture launcher, after images exist.
for service in frontend region-snapshot backend; do
  case "$service" in
    frontend) entry="$repo/frontend/node_modules/vite/bin/vite.js" ;;
    region-snapshot) entry="$repo/backend/dist/workers/region-snapshot.js" ;;
    backend) entry="$repo/backend/dist/index.js" ;;
  esac
  owned_stop_process "$repo/plans/dev-stack/$service.pid" "$entry" 100 0.1 "$service"
done
mapfile -t resident_services < <("${compose[@]}" config --services | rg -v '^(db-migrate|mesh-health-check-data-init|alert-bootstrap|link-backfill-worker|cloudflared)$')
"${compose[@]}" up -d --wait --wait-timeout 240 "${resident_services[@]}"
"${compose[@]}" up -d --no-deps link-backfill-worker
"${compose[@]}" wait link-backfill-worker
backfill_container=$("${compose[@]}" ps -a -q link-backfill-worker)
[[ -n $backfill_container ]] || { echo 'Link backfill container is missing' >&2; exit 1; }
backfill_state=$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$backfill_container")
[[ $backfill_state == exited:0 ]] || { echo 'Link backfill job failed or did not complete' >&2; exit 1; }
printf 'UKMesh http://127.0.0.1:5173\nDashboard http://127.0.0.1:3003\nHealth Check http://127.0.0.1:3090\nGrafana http://127.0.0.1:3010\n'
