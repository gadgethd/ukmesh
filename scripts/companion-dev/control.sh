#!/usr/bin/env bash
# Local fixture stack plus three independently editable companion repositories.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ ${UKMESH_COMPANION_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_COMPANION_ENV_CLEAN=1 bash "$repo/scripts/companion-dev/control.sh" "$@"
fi
export DOCKER_HOST=unix:///var/run/docker.sock
cd "$repo"
action=${1:-start}
service=${2:-}
case "$action" in
  help|--help)
    echo 'scripts/dev-all.sh [start|companions|build|status|stop|logs [SERVICE]|restart SERVICE|init]'
    echo 'start resumes or creates the UKMesh fixture, then starts companions. stop retains all data.'
    echo 'restart rebuilds Go binaries before restarting the named companion. No reset or data deletion.'
    exit 0 ;;
  start|companions|build|status|stop|logs|restart|init|hot) ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
services=(corescope corescope-ingestor beacon beacon-web beacon-db beacon-redis beacon-migrate)
if [[ -n $service ]]; then
  valid=false
  for candidate in "${services[@]}"; do [[ $candidate != "$service" ]] || valid=true; done
  [[ $valid == true ]] || { echo "Unknown companion: $service" >&2; exit 2; }
fi
if [[ $action == restart && -z $service ]]; then echo 'restart requires a companion service name' >&2; exit 2; fi
lock_dir="$repo/plans/companion-dev"
mkdir -p "$lock_dir"
chmod 700 "$lock_dir"
if [[ $action != status && $action != logs && ${UKMESH_COMPANION_LOCKED:-} != 1 ]]; then
  if flock --nonblock --close --conflict-exit-code 75 "$lock_dir/control.lock" \
    env UKMESH_COMPANION_LOCKED=1 bash "$repo/scripts/companion-dev/control.sh" "$@"; then exit 0; else
    result=$?
    if [[ $result == 75 ]]; then echo 'Another DEV operation is running; let it finish before starting a new one.' >&2; fi
    exit "$result"
  fi
fi
if [[ $action == init ]]; then
  for name in corescope beacon-server beacon-web; do
    path="third_party/$name"
    # Preserve existing branches, edits and pending merges.
    if [[ ! -e $path/.git ]]; then git submodule update --init -- "$path"; fi
    if ! git -C "$path" remote get-url upstream >/dev/null 2>&1; then
      case "$name" in
        corescope) upstream=https://github.com/Kpa-clawbot/CoreScope.git ;;
        *) upstream="https://github.com/MeshCore-Beacon/$name.git" ;;
      esac
      git -C "$path" remote add upstream "$upstream"
    fi
    if [[ -z $(git -C "$path" symbolic-ref --short -q HEAD) ]]; then git -C "$path" switch -c ukmesh-dev; fi
  done
  exit 0
fi
command -v docker >/dev/null || { echo 'Docker Engine and Compose are required' >&2; exit 1; }
docker info >/dev/null
export COMPANION_UID=$(id -u) COMPANION_GID=$(id -g)
source "$repo/scripts/full-dev/environment.sh"
compose=(docker compose --project-name ukmesh-companions --env-file /dev/null -f "$repo/scripts/companion-dev/compose.yml")
state="$repo/plans/companion-dev/runtime"
mkdir -p "$state/bin" "$state/corescope" "$state/npm-cache" "$repo/plans/companion-dev/go-cache" "$repo/plans/companion-dev/go-mod"
chmod 700 "$repo/plans/companion-dev" "$state"
case "$action" in
  status)
    "${compose[@]}" ps
    for name in corescope beacon-server beacon-web; do git -C "third_party/$name" status --short --branch; done
    exit 0 ;;
  logs) "${compose[@]}" logs --tail 100 ${service:+"$service"}; exit 0 ;;
  stop)
    "${compose[@]}" stop
    exit 0 ;;
esac
case "$action:$service" in
  start:*|companions:*|build:*|restart:beacon-web)
    python3 "$repo/scripts/companion-dev/artifacts.py" recover-web ;;
esac
# Keep published consumer metadata identical across canonical and proof starts.
source "$repo/scripts/companion-dev/revisions.sh"
load_companion_revisions
projects=(corescope beacon-server beacon-web)
case "$action:$service" in
  restart:corescope|restart:corescope-ingestor) projects=(corescope) ;;
  restart:beacon|restart:beacon-migrate) projects=(beacon-server) ;;
  restart:beacon-web|hot:beacon-web) projects=(beacon-web) ;;
  restart:beacon-db|restart:beacon-redis) projects=() ;;
esac
for name in "${projects[@]}"; do
  [[ -e "third_party/$name/.git" ]] || { echo "Missing $name; run scripts/dev-all.sh init" >&2; exit 1; }
  if [[ -n $(git -C "third_party/$name" diff --name-only --diff-filter=U) ]]; then
    echo "Resolve the $name merge before starting companions" >&2; exit 1
  fi
done
if [[ $action != build ]]; then
  docker network inspect ukmesh-sample_default >/dev/null 2>&1 || { echo 'Start the UKMesh fixture first with scripts/dev-all.sh start' >&2; exit 1; }
fi
components=()
case "$action:$service" in
  build:*|start:*|companions:*) components=(corescope beacon) ;;
  restart:corescope|restart:corescope-ingestor) components=(corescope) ;;
  restart:beacon|restart:beacon-migrate) components=(beacon) ;;
esac
for component in "${components[@]}"; do
  if [[ $component == corescope ]]; then
    COMPANION_CORESCOPE_COMMIT=$(git -C third_party/corescope rev-parse --short HEAD)
    COMPANION_CORESCOPE_SOURCE=$(python3 "$repo/scripts/companion-dev/source-hash.py" corescope)
    export COMPANION_CORESCOPE_COMMIT COMPANION_CORESCOPE_SOURCE
    source_hash=$COMPANION_CORESCOPE_SOURCE
    binaries=(corescope-server corescope-ingestor)
  else
    COMPANION_BEACON_SOURCE=$(python3 "$repo/scripts/companion-dev/source-hash.py" beacon)
    export COMPANION_BEACON_SOURCE
    source_hash=$COMPANION_BEACON_SOURCE
    binaries=(beacon)
  fi
  ready=true
  for executable in "${binaries[@]}"; do [[ -x $state/bin/$executable ]] || ready=false; done
  if [[ $action == build || $ready != true || ! -f $state/$component-source || $(cat "$state/$component-source") != "$source_hash" ]]; then
    "${compose[@]}" run --rm --no-deps build "$component"
    printf '%s\n' "$source_hash" > "$state/$component-source"
  fi
done
if [[ $action == hot ]]; then
  [[ $service == beacon-web ]] || { echo 'Only beacon-web supports companion hot reload' >&2; exit 2; }
  "${compose[@]}" -f "$repo/scripts/companion-dev/hot.yml" up -d --no-deps --force-recreate beacon-web
  exit
fi
if [[ $action == build || $action == start || $action == companions || ( $action == restart && $service == beacon-web ) ]]; then
  web_hash=$(python3 "$repo/scripts/companion-dev/source-hash.py" web)
  if [[ $action == build || ! -f $state/beacon-web/index.html || $COMPANION_WEB_SOURCE != "$web_hash" ]]; then
    python3 "$repo/scripts/companion-dev/artifacts.py" prepare-web
    "${compose[@]}" run --rm --no-deps web-build
    python3 "$repo/scripts/companion-dev/artifacts.py" publish-web "$web_hash"
  fi
  load_companion_revisions
fi
if [[ $action == build ]]; then exit 0; fi
if [[ $action == start || $action == companions || ( $action == restart && $service == beacon* && $service != beacon-web ) ]]; then
  "${compose[@]}" up -d --wait beacon-db beacon-redis
  "${compose[@]}" up -d --no-deps --force-recreate beacon-migrate > "$state/migrate.log" 2>&1 || { tail -30 "$state/migrate.log" >&2; exit 1; }
  migration_wait=0
  "${compose[@]}" wait beacon-migrate >> "$state/migrate.log" 2>&1 || migration_wait=$?
  "${compose[@]}" logs --no-color beacon-migrate >> "$state/migrate.log" 2>&1
  migration_container=$("${compose[@]}" ps -a -q beacon-migrate)
  [[ -n $migration_container ]] || { echo 'Beacon migration container is missing' >&2; exit 1; }
  migration_state=$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$migration_container")
  [[ $migration_wait == 0 && $migration_state == exited:0 ]] || {
    tail -30 "$state/migrate.log" >&2
    echo 'Beacon migration job failed or did not complete' >&2; exit 1;
  }
  bash "$repo/scripts/companion-dev/beacon-access.sh" "$BEACON_DATABASE"
  if [[ $action == restart && $service == beacon-migrate ]]; then exit 0; fi
fi
if [[ $action == restart ]]; then
  targets=("$service")
  # The shared Core build publishes both binaries; keep the running pair on
  # that same revision after replacing either process.
  if [[ $service == corescope || $service == corescope-ingestor ]]; then targets=(corescope-ingestor corescope); fi
  "${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 180 "${targets[@]}"
else
  # Migration already completed above. Keep it out of the explicit up targets,
  # while Compose still orders the resident services' normal dependencies.
  "${compose[@]}" up -d --wait --wait-timeout 240 corescope-ingestor corescope beacon beacon-web
fi
printf 'UKMesh http://127.0.0.1:5173\nCoreScope http://127.0.0.1:3102\nBeacon http://127.0.0.1:5175 (API http://127.0.0.1:3103)\n'
