#!/usr/bin/env bash
# Local fixture stack plus three independently editable companion repositories.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_COMPANION_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_COMPANION_ENV_CLEAN=1 bash "$repo/scripts/dev-all.sh" "$@"
fi
cd "$repo"
action=${1:-start}
service=${2:-}
case "$action" in
  help|--help)
    echo 'scripts/dev-all.sh [start|companions|build|status|stop|logs [SERVICE]|restart SERVICE|init]'
    echo 'start resumes or creates the UKMesh fixture, then starts companions. stop retains all data.'
    echo 'restart rebuilds Go binaries before restarting the named companion. No reset or data deletion.'
    exit 0 ;;
  start|companions|build|status|stop|logs|restart|init) ;;
  *) echo "Unknown action: $action" >&2; exit 2 ;;
esac
services=(corescope corescope-ingestor beacon beacon-web beacon-db beacon-redis)
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
    env UKMESH_COMPANION_LOCKED=1 bash "$repo/scripts/dev-all.sh" "$@"; then exit 0; else
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
compose=(docker compose --env-file /dev/null -f "$repo/scripts/companion-dev/compose.yml")
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
    if [[ -f $repo/plans/dev-stack/fixture-ready ]]; then bash "$repo/scripts/dev-stack.sh" stop; fi
    exit 0 ;;
esac
for name in corescope beacon-server beacon-web; do
  [[ -e "third_party/$name/.git" ]] || { echo "Missing $name; run scripts/dev-all.sh init" >&2; exit 1; }
  if [[ -n $(git -C "third_party/$name" diff --name-only --diff-filter=U) ]]; then
    echo "Resolve the $name merge before starting companions" >&2; exit 1
  fi
done
if [[ $action == start ]]; then
  if ! curl --fail --silent --max-time 3 http://127.0.0.1:3100/healthz >/dev/null; then
    if [[ -f $repo/plans/dev-stack/fixture-ready ]]; then bash "$repo/scripts/dev-stack.sh" start; else bash "$repo/scripts/dev-stack.sh" up; fi
  fi
fi
if [[ $action != build ]]; then
  docker network inspect ukmesh-sample_default >/dev/null 2>&1 || { echo 'Start the UKMesh fixture first with scripts/dev-all.sh start' >&2; exit 1; }
fi
export COMPANION_CORESCOPE_COMMIT=$(git -C third_party/corescope rev-parse --short HEAD)
if [[ $action != restart || $service == corescope || $service == corescope-ingestor || $service == beacon ]]; then
  "${compose[@]}" run --rm --no-deps build
fi
if [[ $action == build ]]; then exit 0; fi
if [[ $action == restart ]]; then
  "${compose[@]}" up -d --wait --wait-timeout 180 "$service"
  "${compose[@]}" restart "$service"
  "${compose[@]}" up -d --wait --wait-timeout 180 "$service"
else
  "${compose[@]}" up -d --wait --wait-timeout 240
fi
printf 'UKMesh http://127.0.0.1:5173\nCoreScope http://127.0.0.1:3102\nBeacon http://127.0.0.1:5175 (API http://127.0.0.1:3103)\n'
